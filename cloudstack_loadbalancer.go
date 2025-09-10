/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloudstack

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/cloudstack-go/v2/cloudstack"
	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cloudprovider "k8s.io/cloud-provider"
)

const (
	// defaultAllowedCIDR is the network range that is allowed on the firewall
	// by default when no explicit CIDR list is given on a LoadBalancer.
	defaultAllowedCIDR = "0.0.0.0/0"

	// ServiceAnnotationLoadBalancerProxyProtocol is the annotation used on the
	// service to enable the proxy protocol on a CloudStack load balancer.
	// Note that this protocol only applies to TCP service ports and
	// CloudStack >= 4.6 is required for it to work.
	ServiceAnnotationLoadBalancerProxyProtocol = "service.beta.kubernetes.io/cloudstack-load-balancer-proxy-protocol"

	ServiceAnnotationLoadBalancerLoadbalancerHostname = "service.beta.kubernetes.io/cloudstack-load-balancer-hostname"
)

type loadBalancer struct {
	*cloudstack.CloudStackClient

	name      string
	algorithm string
	hostIDs   []string
	ipAddr    string
	ipAddrID  string
	networkID string
	projectID string
	rules     map[string]*cloudstack.LoadBalancerRule
}

// GetLoadBalancer returns whether the specified load balancer exists, and if so, what its status is.
func (cs *CSCloud) GetLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service) (*corev1.LoadBalancerStatus, bool, error) {
	klog.V(4).Infof("GetLoadBalancer(%v, %v, %v)", clusterName, service.Namespace, service.Name)

	// Get the load balancer details and existing rules.
	lb, err := cs.getLoadBalancer(service)
	if err != nil {
		return nil, false, err
	}

	// If we don't have any rules, the load balancer does not exist.
	if len(lb.rules) == 0 {
		return nil, false, nil
	}

	klog.V(4).Infof("Found a load balancer associated with IP %v", lb.ipAddr)

	status := &corev1.LoadBalancerStatus{}
	status.Ingress = append(status.Ingress, corev1.LoadBalancerIngress{IP: lb.ipAddr})

	return status, true, nil
}

// EnsureLoadBalancer creates a new load balancer, or updates the existing one. Returns the status of the balancer.
func (cs *CSCloud) EnsureLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) (status *corev1.LoadBalancerStatus, err error) {
	klog.V(2).Infof("EnsureLoadBalancer called for service %s/%s with %d nodes", service.Namespace, service.Name, len(nodes))

	// Debug (condensed): only log node names at higher verbosity
	nodeNames := make([]string, len(nodes))
	for i, node := range nodes {
		nodeNames[i] = node.Name
	}
	klog.V(4).Infof("Nodes passed to EnsureLoadBalancer for service %s/%s: %v", service.Namespace, service.Name, nodeNames)

	if len(service.Spec.Ports) == 0 {
		return nil, fmt.Errorf("requested load balancer with no ports")
	}

	// Get the load balancer details and existing rules.
	lb, err := cs.getLoadBalancer(service)
	if err != nil {
		return nil, err
	}

	// Set the load balancer algorithm.
	switch service.Spec.SessionAffinity {
	case corev1.ServiceAffinityNone:
		lb.algorithm = "roundrobin"
	case corev1.ServiceAffinityClientIP:
		lb.algorithm = "source"
	default:
		return nil, fmt.Errorf("unsupported load balancer affinity: %v", service.Spec.SessionAffinity)
	}

	// Filter nodes based on externalTrafficPolicy (auto-includes control-plane nodes with endpoints when needed)
	filteredNodes := cs.filterNodesForExternalTrafficPolicy(service, nodes)
	klog.V(2).Infof("Service %s/%s: filtered %d nodes to %d based on traffic policy",
		service.Namespace, service.Name, len(nodes), len(filteredNodes))

	// Verify that all the hosts belong to the same network, and retrieve their ID's.
	lb.hostIDs, lb.networkID, err = cs.verifyHosts(filteredNodes)
	if err != nil {
		return nil, err
	}

	klog.V(2).Infof("Service %s/%s: expected host IDs: %v", service.Namespace, service.Name, lb.hostIDs)

	if !lb.hasLoadBalancerIP() {
		// Create or retrieve the load balancer IP.
		if err := lb.getLoadBalancerIP(service.Spec.LoadBalancerIP); err != nil {
			return nil, err
		}

		if lb.ipAddr != "" && lb.ipAddr != service.Spec.LoadBalancerIP {
			defer func(lb *loadBalancer) {
				if err != nil {
					if err := lb.releaseLoadBalancerIP(); err != nil {
						klog.Errorf(err.Error())
					}
				}
			}(lb)
		}
	}

	klog.V(4).Infof("Load balancer %v is associated with IP %v", lb.name, lb.ipAddr)

	for _, port := range service.Spec.Ports {
		// Construct the protocol name first, we need it a few times
		protocol := ProtocolFromServicePort(port, service)
		if protocol == LoadBalancerProtocolInvalid {
			return nil, fmt.Errorf("unsupported load balancer protocol: %v", port.Protocol)
		}

		// All ports have their own load balancer rule, so add the port to lbName to keep the names unique.
		lbRuleName := fmt.Sprintf("%s-%s-%d", lb.name, protocol, port.Port)

		// If the load balancer rule exists and is up-to-date, we move on to the next rule.
		lbRule, needsUpdate, err := lb.checkLoadBalancerRule(lbRuleName, port, protocol)
		if err != nil {
			return nil, err
		}

		// Ensure the existing rule (if any) is on the expected network
		if lbRule != nil {
			if valid, validationErr := lb.validateLoadBalancerRuleNetwork(lbRule); !valid {
				klog.Warningf("Load balancer rule %s has invalid network configuration: %v", lbRuleName, validationErr)
				klog.Infof("Recreating load balancer rule %s due to network mismatch", lbRuleName)

				if deleteErr := lb.deleteLoadBalancerRule(lbRule); deleteErr != nil {
					return nil, fmt.Errorf("failed to delete load balancer rule %s during network validation: %v", lbRuleName, deleteErr)
				}

				lbRule = nil
				needsUpdate = false
			}
		}

		if lbRule != nil {
			if len(lb.hostIDs) == 0 {
				klog.V(2).Infof("Service %s/%s rule %s: desired host list empty, keeping current members to avoid churn", service.Namespace, service.Name, lbRuleName)
				delete(lb.rules, lbRuleName)
				continue
			}

			if needsUpdate {
				klog.V(2).Infof("Updating load balancer rule: %v", lbRuleName)
				if err := lb.updateLoadBalancerRule(lbRuleName, protocol); err != nil {
					return nil, err
				}
			}

			// CRITICAL FIX: Check and update existing rule members for endpoint changes
			klog.V(2).Infof("Checking members for existing load balancer rule: %v", lbRuleName)

			// Get current members of this rule
			p := lb.LoadBalancer.NewListLoadBalancerRuleInstancesParams(lbRule.Id)
			l, err := lb.LoadBalancer.ListLoadBalancerRuleInstances(p)
			if err != nil {
				return nil, fmt.Errorf("error retrieving associated instances for rule %s: %v", lbRuleName, err)
			}

			// Log current members
			currentMembers := make([]string, len(l.LoadBalancerRuleInstances))
			for i, instance := range l.LoadBalancerRuleInstances {
				currentMembers[i] = instance.Id
			}
			klog.V(2).Infof("Service %s/%s rule %s: current members: %v, expected: %v",
				service.Namespace, service.Name, lbRuleName, currentMembers, lb.hostIDs)

			// Check if members need updating
			assign, remove := symmetricDifference(lb.hostIDs, l.LoadBalancerRuleInstances)

			if len(assign) > 0 {
				klog.Infof("Updating members of load balancer rule %s: adding %d hosts %v", lbRuleName, len(assign), assign)
				klog.V(2).Infof("Service %s/%s rule %s: assigning new hosts: %v",
					service.Namespace, service.Name, lbRuleName, assign)
				if err := lb.assignHostsToRule(lbRule, assign); err != nil {
					return nil, err
				}
			}

			if len(remove) > 0 {
				klog.Infof("Updating members of load balancer rule %s: removing %d hosts %v", lbRuleName, len(remove), remove)
				klog.V(2).Infof("Service %s/%s rule %s: removing old hosts: %v",
					service.Namespace, service.Name, lbRuleName, remove)
				if err := lb.removeHostsFromRule(lbRule, remove); err != nil {
					return nil, err
				}
			}

			if len(assign) == 0 && len(remove) == 0 {
				klog.V(2).Infof("Service %s/%s rule %s: members are up-to-date",
					service.Namespace, service.Name, lbRuleName)
			}

			// Delete the rule from the map, to prevent it being deleted.
			delete(lb.rules, lbRuleName)
		} else {
			klog.V(2).Infof("Creating load balancer rule: %v", lbRuleName)
			lbRule, err = lb.createLoadBalancerRule(lbRuleName, port, protocol)
			if err != nil {
				return nil, err
			}

			if len(lb.hostIDs) == 0 {
				klog.V(2).Infof("Service %s/%s rule %s: created without members (no desired hosts)", service.Namespace, service.Name, lbRuleName)
				delete(lb.rules, lbRuleName)
				continue
			}

			// Only assign hosts if we have any
			klog.Infof("Creating members for new load balancer rule %s: adding %d hosts %v", lbRuleName, len(lb.hostIDs), lb.hostIDs)
			klog.V(2).Infof("Assigning hosts (%v) to load balancer rule: %v", lb.hostIDs, lbRuleName)
			if err = lb.assignHostsToRule(lbRule, lb.hostIDs); err != nil {
				return nil, err
			}
		}

		network, count, err := lb.Network.GetNetworkByID(lb.networkID, cloudstack.WithProject(lb.projectID))
		if err != nil {
			if count == 0 {
				return nil, err
			}
			return nil, err
		}

		if lbRule != nil {
			if isFirewallSupported(network.Service) {
				klog.V(4).Infof("Creating firewall rules for load balancer rule: %v (%v:%v:%v)", lbRuleName, protocol, lbRule.Publicip, port.Port)
				if _, err := lb.updateFirewallRule(lbRule.Publicipid, int(port.Port), protocol, service.Spec.LoadBalancerSourceRanges); err != nil {
					return nil, err
				}
			} else if isNetworkACLSupported(network.Service) {
				klog.V(4).Infof("Creating ACL rules for load balancer rule: %v (%v:%v:%v)", lbRuleName, protocol, lbRule.Publicip, port.Port)
				if _, err := lb.updateNetworkACL(int(port.Port), protocol, network.Id); err != nil {
					return nil, err
				}
			}
		}
	}

	// Cleanup any rules that are now still in the rules map, as they are no longer needed.
	for _, lbRule := range lb.rules {
		protocol := ProtocolFromLoadBalancer(lbRule.Protocol)
		if protocol == LoadBalancerProtocolInvalid {
			return nil, fmt.Errorf("error parsing protocol %v: %v", lbRule.Protocol, err)
		}
		port, err := strconv.ParseInt(lbRule.Publicport, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("error parsing port %s: %v", lbRule.Publicport, err)
		}

		klog.V(4).Infof("Deleting firewall rules associated with load balancer rule: %v (%v:%v:%v)", lbRule.Name, protocol, lbRule.Publicip, port)
		if _, err := lb.deleteFirewallRule(lbRule.Publicipid, int(port), protocol); err != nil {
			return nil, err
		}

		klog.V(4).Infof("Deleting Network ACL rules associated with load balancer rule: %v (%v:%v)", lbRule.Name, protocol, port)
		if _, err := lb.deleteNetworkACLRule(int(port), protocol, lb.networkID); err != nil {
			return nil, err
		}

		klog.V(4).Infof("Deleting obsolete load balancer rule: %v", lbRule.Name)
		if err := lb.deleteLoadBalancerRule(lbRule); err != nil {
			return nil, err
		}
	}

	status = &corev1.LoadBalancerStatus{}
	// If hostname is explicitly set using service annotation
	// Workaround for https://github.com/kubernetes/kubernetes/issues/66607
	if hostname := getStringFromServiceAnnotation(service, ServiceAnnotationLoadBalancerLoadbalancerHostname, ""); hostname != "" {
		status.Ingress = []corev1.LoadBalancerIngress{{Hostname: hostname}}
		return status, nil
	}
	// Default to IP
	status.Ingress = []corev1.LoadBalancerIngress{{IP: lb.ipAddr}}

	return status, nil
}

// UpdateLoadBalancer updates hosts under the specified load balancer.
func (cs *CSCloud) UpdateLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) error {
	klog.V(2).Infof("UpdateLoadBalancer called for service %s/%s with %d nodes", service.Namespace, service.Name, len(nodes))

	// Get the load balancer details and existing rules.
	lb, err := cs.getLoadBalancer(service)
	if err != nil {
		return err
	}

	// Filter nodes based on externalTrafficPolicy
	filteredNodes := cs.filterNodesForExternalTrafficPolicy(service, nodes)
	klog.V(2).Infof("Service %s/%s: filtered %d nodes to %d based on traffic policy",
		service.Namespace, service.Name, len(nodes), len(filteredNodes))

	// Verify that all the hosts belong to the same network, and retrieve their ID's.
	lb.hostIDs, _, err = cs.verifyHosts(filteredNodes)
	if err != nil {
		return err
	}

	klog.V(2).Infof("Service %s/%s: expected host IDs: %v", service.Namespace, service.Name, lb.hostIDs)

	updatesMade := false
	for _, lbRule := range lb.rules {
		p := lb.LoadBalancer.NewListLoadBalancerRuleInstancesParams(lbRule.Id)

		// Retrieve all VMs currently associated to this load balancer rule.
		l, err := lb.LoadBalancer.ListLoadBalancerRuleInstances(p)
		if err != nil {
			return fmt.Errorf("error retrieving associated instances: %v", err)
		}

		// Log current members
		currentMembers := make([]string, len(l.LoadBalancerRuleInstances))
		for i, instance := range l.LoadBalancerRuleInstances {
			currentMembers[i] = instance.Id
		}
		klog.V(2).Infof("Service %s/%s rule %s: current members: %v",
			service.Namespace, service.Name, lbRule.Name, currentMembers)

		assign, remove := symmetricDifference(lb.hostIDs, l.LoadBalancerRuleInstances)

		if len(assign) > 0 {
			klog.Infof("Updating members of load balancer rule %s: adding %d hosts %v", lbRule.Name, len(assign), assign)
			klog.V(2).Infof("Service %s/%s rule %s: assigning new hosts: %v",
				service.Namespace, service.Name, lbRule.Name, assign)
			if err := lb.assignHostsToRule(lbRule, assign); err != nil {
				return err
			}
			updatesMade = true
		}

		if len(remove) > 0 {
			klog.Infof("Updating members of load balancer rule %s: removing %d hosts %v", lbRule.Name, len(remove), remove)
			klog.V(2).Infof("Service %s/%s rule %s: removing old hosts: %v",
				service.Namespace, service.Name, lbRule.Name, remove)
			if err := lb.removeHostsFromRule(lbRule, remove); err != nil {
				return err
			}
			updatesMade = true
		}
	}

	if updatesMade {
		klog.V(1).Infof("Successfully updated load balancer members for service %s/%s", service.Namespace, service.Name)
	} else {
		klog.V(2).Infof("No updates needed for service %s/%s load balancer", service.Namespace, service.Name)
	}

	return nil
}

func isFirewallSupported(services []cloudstack.NetworkServiceInternal) bool {
	for _, svc := range services {
		if svc.Name == "Firewall" {
			return true
		}
	}
	return false
}

func isNetworkACLSupported(services []cloudstack.NetworkServiceInternal) bool {
	for _, svc := range services {
		if svc.Name == "NetworkACL" {
			return true
		}
	}
	return false
}

// EnsureLoadBalancerDeleted deletes the specified load balancer if it exists, returning
// nil if the load balancer specified either didn't exist or was successfully deleted.
func (cs *CSCloud) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *corev1.Service) error {
	klog.V(4).Infof("EnsureLoadBalancerDeleted(%v, %v, %v)", clusterName, service.Namespace, service.Name)

	// Get the load balancer details and existing rules.
	lb, err := cs.getLoadBalancer(service)
	if err != nil {
		return err
	}

	for _, lbRule := range lb.rules {
		klog.V(4).Infof("Deleting firewall rules / Network ACLs for load balancer: %v", lbRule.Name)
		protocol := ProtocolFromLoadBalancer(lbRule.Protocol)
		if protocol == LoadBalancerProtocolInvalid {
			klog.Errorf("Error parsing protocol: %v", lbRule.Protocol)
		} else {
			port, err := strconv.ParseInt(lbRule.Publicport, 10, 32)
			if err != nil {
				klog.Errorf("Error parsing port: %v", err)
			} else {
				networkId, err := cs.getNetworkIDFromIPAddress(lb.ipAddrID)
				if err != nil {
					klog.Errorf("Failed to get network ID from IP address %v: %v", lb.ipAddrID, err)
					// Continue with deletion of load balancer rule even if network cleanup fails
				} else {
					network, count, err := lb.Network.GetNetworkByID(networkId, cloudstack.WithProject(lb.projectID))
					if err != nil {
						if count == 0 {
							klog.Errorf("No network found with ID: %v", networkId)
						} else {
							klog.Errorf("Error fetching network with ID: %v, error: %v", networkId, err)
						}
						// Continue with deletion of load balancer rule even if network cleanup fails
					} else {
						if network.Vpcid == "" {
							_, err = lb.deleteFirewallRule(lbRule.Publicipid, int(port), protocol)
							if err != nil {
								klog.Errorf("Error deleting firewall rule: %v", err)
							}
						} else {
							klog.V(4).Infof("Deleting network ACLs for %v - %v", int(port), protocol)
							_, err = lb.deleteNetworkACLRule(int(port), protocol, networkId)
							if err != nil {
								klog.Errorf("Error deleting Network ACL rule: %v", err)
							}
						}
					}
				}
			}

			klog.V(4).Infof("Deleting load balancer rule: %v", lbRule.Name)
			if err := lb.deleteLoadBalancerRule(lbRule); err != nil {
				return err
			}
		}
	}

	if lb.ipAddr != "" && lb.ipAddr != service.Spec.LoadBalancerIP {
		klog.V(4).Infof("Releasing load balancer IP: %v", lb.ipAddr)
		if err := lb.releaseLoadBalancerIP(); err != nil {
			return err
		}
	}

	return nil
}

// GetLoadBalancerName retrieves the name of the LoadBalancer.
func (cs *CSCloud) GetLoadBalancerName(ctx context.Context, clusterName string, service *corev1.Service) string {
	return cloudprovider.DefaultLoadBalancerName(service)
}

// getLoadBalancer retrieves the IP address and ID and all the existing rules it can find.
func (cs *CSCloud) getLoadBalancer(service *corev1.Service) (*loadBalancer, error) {
	lb := &loadBalancer{
		CloudStackClient: cs.client,
		name:             cs.GetLoadBalancerName(context.TODO(), "", service),
		projectID:        cs.projectID,
		rules:            make(map[string]*cloudstack.LoadBalancerRule),
	}

	p := cs.client.LoadBalancer.NewListLoadBalancerRulesParams()
	p.SetKeyword(lb.name)
	p.SetListall(true)

	if cs.projectID != "" {
		p.SetProjectid(cs.projectID)
	}

	l, err := cs.client.LoadBalancer.ListLoadBalancerRules(p)
	if err != nil {
		return nil, fmt.Errorf("error retrieving load balancer rules: %v", err)
	}

	for _, lbRule := range l.LoadBalancerRules {
		lb.rules[lbRule.Name] = lbRule

		if lb.ipAddr != "" && lb.ipAddr != lbRule.Publicip {
			klog.Warningf("Load balancer for service %v/%v has rules associated with different IP's: %v, %v", service.Namespace, service.Name, lb.ipAddr, lbRule.Publicip)
		}

		lb.ipAddr = lbRule.Publicip
		lb.ipAddrID = lbRule.Publicipid
	}

	klog.V(4).Infof("Load balancer %v contains %d rule(s)", lb.name, len(lb.rules))

	return lb, nil
}

// Get network ID from Public IP Address
func (cs *CSCloud) getNetworkIDFromIPAddress(publicIpId string) (string, error) {
	if publicIpId == "" {
		return "", fmt.Errorf("public IP ID is empty")
	}

	ip, count, err := cs.client.Address.GetPublicIpAddressByID(publicIpId, cloudstack.WithProject(cs.projectID))
	if err != nil {
		klog.Errorf("Failed to fetch the public IP for id: %v", publicIpId)
		return "", err
	}
	if count == 0 {
		return "", fmt.Errorf("no public IP found with ID: %v", publicIpId)
	}
	if ip.Associatednetworkid != "" {
		network, _, netErr := cs.client.Network.GetNetworkByID(ip.Associatednetworkid, cloudstack.WithProject(cs.projectID))
		if netErr != nil {
			klog.Errorf("Failed to fetch the network for id: %v", ip.Associatednetworkid)
			return "", netErr
		}
		return network.Id, nil
	}
	return "", fmt.Errorf("public IP %v is not associated with any network", publicIpId)
}

// getDefaultNetworkID returns a default network ID for creating load balancers without members
// It tries to find the network used by Kubernetes cluster nodes
func (cs *CSCloud) getDefaultNetworkID() (string, error) {
	// First try to get the network from Kubernetes cluster nodes
	if cs.kubeClient != nil {
		klog.V(4).Infof("getDefaultNetworkID: Trying to find network from Kubernetes cluster nodes")

		// Get all nodes from the cluster
		allNodes, err := cs.kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		if err == nil && len(allNodes.Items) > 0 {
			// Try to find the network from cluster nodes
			for _, node := range allNodes.Items {
				// Get VM details for this node
				vm, count, err := cs.client.VirtualMachine.GetVirtualMachineByName(
					strings.Split(strings.ToLower(node.Name), ".")[0],
					cloudstack.WithProject(cs.projectID),
				)
				if err == nil && count > 0 && len(vm.Nic) > 0 {
					klog.V(4).Infof("getDefaultNetworkID: Using network %s from Kubernetes node %s (VM: %s)",
						vm.Nic[0].Networkid, node.Name, vm.Name)
					return vm.Nic[0].Networkid, nil
				}
			}
		}
		klog.V(4).Infof("getDefaultNetworkID: Could not find network from Kubernetes nodes, falling back to VM list")
	}

	// Fallback: get networks from any existing VMs in the project/zone
	p := cs.client.VirtualMachine.NewListVirtualMachinesParams()
	p.SetListall(true)
	p.SetDetails([]string{"min", "nics"})

	if cs.projectID != "" {
		p.SetProjectid(cs.projectID)
	}

	l, err := cs.client.VirtualMachine.ListVirtualMachines(p)
	if err == nil && len(l.VirtualMachines) > 0 {
		// Use the network from the first VM found
		for _, vm := range l.VirtualMachines {
			if len(vm.Nic) > 0 {
				klog.V(4).Infof("getDefaultNetworkID: Using network %s from existing VM %s", vm.Nic[0].Networkid, vm.Name)
				return vm.Nic[0].Networkid, nil
			}
		}
	}

	// Final fallback: list networks directly
	netParams := cs.client.Network.NewListNetworksParams()
	if cs.projectID != "" {
		netParams.SetProjectid(cs.projectID)
	}

	networks, err := cs.client.Network.ListNetworks(netParams)
	if err != nil {
		return "", fmt.Errorf("failed to list networks: %v", err)
	}

	if len(networks.Networks) == 0 {
		return "", fmt.Errorf("no networks found in project")
	}

	// Use the first network found
	networkID := networks.Networks[0].Id
	klog.V(4).Infof("getDefaultNetworkID: Using first available network %s (%s)", networkID, networks.Networks[0].Name)
	return networkID, nil
}

// verifyHosts verifies if all hosts belong to the same network, and returns the host ID's and network ID.
func (cs *CSCloud) verifyHosts(nodes []*corev1.Node) ([]string, string, error) {
	// Handle case where no nodes are provided (empty load balancer)
	if len(nodes) == 0 {
		klog.V(2).Infof("verifyHosts: No nodes provided - creating load balancer without members")
		// Return empty hostIDs but we still need a networkID for load balancer creation
		// We'll use the first available network from the project/zone
		networkID, err := cs.getDefaultNetworkID()
		if err != nil {
			return nil, "", fmt.Errorf("failed to get default network ID for empty load balancer: %v", err)
		}
		return []string{}, networkID, nil
	}

	hostNames := map[string]bool{}
	nodeNames := make([]string, len(nodes))
	for i, node := range nodes {
		nodeNames[i] = node.Name
		hostNames[strings.Split(strings.ToLower(node.Name), ".")[0]] = true
	}

	klog.V(4).Infof("verifyHosts: Looking for nodes %v in CloudStack VMs", nodeNames)

	p := cs.client.VirtualMachine.NewListVirtualMachinesParams()
	p.SetListall(true)
	p.SetDetails([]string{"min", "nics"})

	if cs.projectID != "" {
		p.SetProjectid(cs.projectID)
	}

	l, err := cs.client.VirtualMachine.ListVirtualMachines(p)
	if err != nil {
		return nil, "", fmt.Errorf("error retrieving list of hosts: %v", err)
	}

	// Debug: Log all available CloudStack VMs at lower verbosity
	vmNames := make([]string, len(l.VirtualMachines))
	for i, vm := range l.VirtualMachines {
		vmNames[i] = vm.Name
	}
	klog.V(5).Infof("verifyHosts: Available CloudStack VMs: %v", vmNames)

	var hostIDs []string
	var networkID string

	// Check if the virtual machine is in the hosts slice, then add the corresponding ID.
	for _, vm := range l.VirtualMachines {
		if hostNames[strings.ToLower(vm.Name)] {
			if networkID != "" && networkID != vm.Nic[0].Networkid {
				return nil, "", fmt.Errorf("found hosts that belong to different networks")
			}

			networkID = vm.Nic[0].Networkid
			hostIDs = append(hostIDs, vm.Id)
		}
	}

	if len(hostIDs) == 0 || len(networkID) == 0 {
		klog.V(3).Infof("verifyHosts: No matches. Kubernetes nodes: %v; CloudStack VMs: %v", nodeNames, vmNames)
		return nil, "", fmt.Errorf("none of the hosts matched the list of VMs retrieved from CS API")
	}

	return hostIDs, networkID, nil
}

// hasLoadBalancerIP returns true if we have a load balancer address and ID.
func (lb *loadBalancer) hasLoadBalancerIP() bool {
	return lb.ipAddr != "" && lb.ipAddrID != ""
}

// getLoadBalancerIP retrieves an existing IP or associates a new IP.
func (lb *loadBalancer) getLoadBalancerIP(loadBalancerIP string) error {
	if loadBalancerIP != "" {
		return lb.getPublicIPAddress(loadBalancerIP)
	}

	return lb.associatePublicIPAddress()
}

// getPublicIPAddressID retrieves the ID of the given IP, and sets the address and it's ID.
func (lb *loadBalancer) getPublicIPAddress(loadBalancerIP string) error {
	klog.V(4).Infof("Retrieve load balancer IP details: %v", loadBalancerIP)

	p := lb.Address.NewListPublicIpAddressesParams()
	p.SetIpaddress(loadBalancerIP)
	p.SetListall(true)

	if lb.projectID != "" {
		p.SetProjectid(lb.projectID)
	}

	l, err := lb.Address.ListPublicIpAddresses(p)
	if err != nil {
		return fmt.Errorf("error retrieving IP address: %v", err)
	}

	if l.Count != 1 {
		return fmt.Errorf("could not find IP address %v", loadBalancerIP)
	}

	lb.ipAddr = l.PublicIpAddresses[0].Ipaddress
	lb.ipAddrID = l.PublicIpAddresses[0].Id

	return nil
}

// associatePublicIPAddress associates a new IP and sets the address and it's ID.
func (lb *loadBalancer) associatePublicIPAddress() error {
	klog.V(4).Infof("Allocate new IP for load balancer: %v", lb.name)
	// If a network belongs to a VPC, the IP address needs to be associated with
	// the VPC instead of with the network.
	network, count, err := lb.Network.GetNetworkByID(lb.networkID, cloudstack.WithProject(lb.projectID))
	if err != nil {
		if count == 0 {
			return fmt.Errorf("could not find network %v", lb.networkID)
		}
		return fmt.Errorf("error retrieving network: %v", err)
	}

	p := lb.Address.NewAssociateIpAddressParams()

	if network.Vpcid != "" {
		p.SetVpcid(network.Vpcid)
	} else {
		p.SetNetworkid(lb.networkID)
	}

	if lb.projectID != "" {
		p.SetProjectid(lb.projectID)
	}

	// Associate a new IP address
	r, err := lb.Address.AssociateIpAddress(p)
	if err != nil {
		return fmt.Errorf("error associating new IP address: %v", err)
	}

	lb.ipAddr = r.Ipaddress
	lb.ipAddrID = r.Id

	return nil
}

// releasePublicIPAddress releases an associated IP.
func (lb *loadBalancer) releaseLoadBalancerIP() error {
	p := lb.Address.NewDisassociateIpAddressParams(lb.ipAddrID)

	if _, err := lb.Address.DisassociateIpAddress(p); err != nil {
		return fmt.Errorf("error releasing load balancer IP %v: %v", lb.ipAddr, err)
	}

	return nil
}

// checkLoadBalancerRule checks if the rule already exists and if it does, if it can be updated. If
// it does exist but cannot be updated, it will delete the existing rule so it can be created again.
func (lb *loadBalancer) checkLoadBalancerRule(lbRuleName string, port corev1.ServicePort, protocol LoadBalancerProtocol) (*cloudstack.LoadBalancerRule, bool, error) {
	lbRule, ok := lb.rules[lbRuleName]
	if !ok {
		return nil, false, nil
	}

	// Check if any of the values we cannot update (those that require a new load balancer rule) are changed.
	if lbRule.Publicip == lb.ipAddr && lbRule.Privateport == strconv.Itoa(int(port.NodePort)) && lbRule.Publicport == strconv.Itoa(int(port.Port)) {
		updateAlgo := lbRule.Algorithm != lb.algorithm
		updateProto := lbRule.Protocol != protocol.CSProtocol()
		return lbRule, updateAlgo || updateProto, nil
	}

	// Delete the load balancer rule so we can create a new one using the new values.
	if err := lb.deleteLoadBalancerRule(lbRule); err != nil {
		return nil, false, err
	}

	return nil, false, nil
}

// updateLoadBalancerRule updates a load balancer rule.
func (lb *loadBalancer) updateLoadBalancerRule(lbRuleName string, protocol LoadBalancerProtocol) error {
	lbRule := lb.rules[lbRuleName]

	p := lb.LoadBalancer.NewUpdateLoadBalancerRuleParams(lbRule.Id)
	p.SetAlgorithm(lb.algorithm)
	p.SetProtocol(protocol.CSProtocol())

	_, err := lb.LoadBalancer.UpdateLoadBalancerRule(p)
	return err
}

// createLoadBalancerRule creates a new load balancer rule and returns it's ID.
func (lb *loadBalancer) createLoadBalancerRule(lbRuleName string, port corev1.ServicePort, protocol LoadBalancerProtocol) (*cloudstack.LoadBalancerRule, error) {
	p := lb.LoadBalancer.NewCreateLoadBalancerRuleParams(
		lb.algorithm,
		lbRuleName,
		int(port.NodePort),
		int(port.Port),
	)

	p.SetNetworkid(lb.networkID)
	p.SetPublicipid(lb.ipAddrID)

	p.SetProtocol(protocol.CSProtocol())

	// Do not open the firewall implicitly, we always create explicit firewall rules
	p.SetOpenfirewall(false)

	// Create a new load balancer rule.
	r, err := lb.LoadBalancer.CreateLoadBalancerRule(p)
	if err != nil {
		return nil, fmt.Errorf("error creating load balancer rule %v: %v", lbRuleName, err)
	}

	lbRule := &cloudstack.LoadBalancerRule{
		Id:          r.Id,
		Algorithm:   r.Algorithm,
		Cidrlist:    r.Cidrlist,
		Name:        r.Name,
		Networkid:   r.Networkid,
		Privateport: r.Privateport,
		Publicport:  r.Publicport,
		Publicip:    r.Publicip,
		Publicipid:  r.Publicipid,
		Protocol:    r.Protocol,
	}

	return lbRule, nil
}

// deleteLoadBalancerRule deletes a load balancer rule.
func (lb *loadBalancer) deleteLoadBalancerRule(lbRule *cloudstack.LoadBalancerRule) error {
	p := lb.LoadBalancer.NewDeleteLoadBalancerRuleParams(lbRule.Id)

	if _, err := lb.LoadBalancer.DeleteLoadBalancerRule(p); err != nil {
		return fmt.Errorf("error deleting load balancer rule %v: %v", lbRule.Name, err)
	}

	// Delete the rule from the map as it no longer exists
	delete(lb.rules, lbRule.Name)

	return nil
}

// assignHostsToRule assigns hosts to a load balancer rule.
func (lb *loadBalancer) assignHostsToRule(lbRule *cloudstack.LoadBalancerRule, hostIDs []string) error {
	p := lb.LoadBalancer.NewAssignToLoadBalancerRuleParams(lbRule.Id)
	p.SetVirtualmachineids(hostIDs)

	if _, err := lb.LoadBalancer.AssignToLoadBalancerRule(p); err != nil {
		return fmt.Errorf("error assigning hosts to load balancer rule %v: %v", lbRule.Name, err)
	}

	return nil
}

// removeHostsFromRule removes hosts from a load balancer rule.
func (lb *loadBalancer) removeHostsFromRule(lbRule *cloudstack.LoadBalancerRule, hostIDs []string) error {
	p := lb.LoadBalancer.NewRemoveFromLoadBalancerRuleParams(lbRule.Id)
	p.SetVirtualmachineids(hostIDs)

	if _, err := lb.LoadBalancer.RemoveFromLoadBalancerRule(p); err != nil {
		return fmt.Errorf("error removing hosts from load balancer rule %v: %v", lbRule.Name, err)
	}

	return nil
}

// symmetricDifference returns the symmetric difference between the old (existing) and new (wanted) host ID's.
func symmetricDifference(hostIDs []string, lbInstances []*cloudstack.VirtualMachine) ([]string, []string) {
	new := make(map[string]bool)
	for _, hostID := range hostIDs {
		new[hostID] = true
	}

	var remove []string
	for _, instance := range lbInstances {
		if new[instance.Id] {
			delete(new, instance.Id)
			continue
		}

		remove = append(remove, instance.Id)
	}

	var assign []string
	for hostID := range new {
		assign = append(assign, hostID)
	}

	return assign, remove
}

// compareStringSlice compares two unsorted slices of strings without sorting them first.
//
// The slices are equal if and only if both contain the same number of every unique element.
//
// Thanks to: https://stackoverflow.com/a/36000696
func compareStringSlice(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	// create a map of string -> int
	diff := make(map[string]int, len(x))
	for _, _x := range x {
		// 0 value for int is 0, so just increment a counter for the string
		diff[_x]++
	}
	for _, _y := range y {
		// If the string _y is not in diff bail out early
		if _, ok := diff[_y]; !ok {
			return false
		}
		diff[_y] -= 1
		if diff[_y] == 0 {
			delete(diff, _y)
		}
	}
	return len(diff) == 0
}

func ruleToString(rule *cloudstack.FirewallRule) string {
	ls := &strings.Builder{}
	if rule == nil {
		ls.WriteString("nil")
	} else {
		switch rule.Protocol {
		case "tcp":
			fallthrough
		case "udp":
			fmt.Fprintf(ls, "{[%s] -> %s:[%d-%d] (%s)}", rule.Cidrlist, rule.Ipaddress, rule.Startport, rule.Endport, rule.Protocol)
		case "icmp":
			fmt.Fprintf(ls, "{[%s] -> %s [%d,%d] (%s)}", rule.Cidrlist, rule.Ipaddress, rule.Icmptype, rule.Icmpcode, rule.Protocol)
		default:
			fmt.Fprintf(ls, "{[%s] -> %s (%s)}", rule.Cidrlist, rule.Ipaddress, rule.Protocol)
		}
	}
	return ls.String()
}

func rulesToString(rules []*cloudstack.FirewallRule) string {
	ls := &strings.Builder{}
	first := true
	for _, rule := range rules {
		if first {
			first = false
		} else {
			ls.WriteString(", ")
		}
		ls.WriteString(ruleToString(rule))
	}
	return ls.String()
}

func rulesMapToString(rules map[*cloudstack.FirewallRule]bool) string {
	ls := &strings.Builder{}
	first := true
	for rule := range rules {
		if first {
			first = false
		} else {
			ls.WriteString(", ")
		}
		ls.WriteString(ruleToString(rule))
	}
	return ls.String()
}

// updateFirewallRule creates a firewall rule for a load balancer rule
//
// If the rule list is empty, all internet (IPv4: 0.0.0.0/0) is opened for the
// load balancer's port+protocol implicitly.
//
// Returns true if the firewall rule was created or updated
func (lb *loadBalancer) updateFirewallRule(publicIpId string, publicPort int, protocol LoadBalancerProtocol, allowedIPs []string) (bool, error) {
	if len(allowedIPs) == 0 {
		allowedIPs = []string{defaultAllowedCIDR}
	}

	p := lb.Firewall.NewListFirewallRulesParams()
	p.SetIpaddressid(publicIpId)
	p.SetListall(true)
	if lb.projectID != "" {
		p.SetProjectid(lb.projectID)
	}
	klog.V(4).Infof("Listing firewall rules for %v", p)
	r, err := lb.Firewall.ListFirewallRules(p)
	if err != nil {
		return false, fmt.Errorf("error fetching firewall rules for public IP %v: %v", publicIpId, err)
	}
	klog.V(4).Infof("All firewall rules for %v: %v", lb.ipAddr, rulesToString(r.FirewallRules))

	// find all rules that have a matching proto+port
	// a map may or may not be faster, but is a bit easier to understand
	filtered := make(map[*cloudstack.FirewallRule]bool)
	for _, rule := range r.FirewallRules {
		if rule.Protocol == protocol.IPProtocol() && rule.Startport == publicPort && rule.Endport == publicPort {
			filtered[rule] = true
		}
	}
	klog.V(4).Infof("Matching rules for %v: %v", lb.ipAddr, rulesMapToString(filtered))

	// determine if we already have a rule with matching cidrs
	var match *cloudstack.FirewallRule
	for rule := range filtered {
		cidrlist := strings.Split(rule.Cidrlist, ",")
		if compareStringSlice(cidrlist, allowedIPs) {
			klog.V(4).Infof("Found identical rule: %v", rule)
			match = rule
			break
		}
	}

	if match != nil {
		// no need to create a new rule - but prevent deletion of the matching rule
		delete(filtered, match)
	}

	// delete all other rules that didn't match the CIDR list
	// do this first to prevent CS rule conflict errors
	klog.V(4).Infof("Firewall rules to be deleted for %v: %v", lb.ipAddr, rulesMapToString(filtered))
	for rule := range filtered {
		p := lb.Firewall.NewDeleteFirewallRuleParams(rule.Id)
		_, err = lb.Firewall.DeleteFirewallRule(p)
		if err != nil {
			// report the error, but keep on deleting the other rules
			klog.Errorf("Error deleting old firewall rule %v: %v", rule.Id, err)
		}
	}

	// create new rule if necessary
	if match == nil {
		// no rule found, create a new one
		p := lb.Firewall.NewCreateFirewallRuleParams(publicIpId, protocol.IPProtocol())
		p.SetCidrlist(allowedIPs)
		p.SetStartport(publicPort)
		p.SetEndport(publicPort)
		_, err = lb.Firewall.CreateFirewallRule(p)
		if err != nil {
			// return immediately if we can't create the new rule
			return false, fmt.Errorf("error creating new firewall rule for public IP %v, proto %v, port %v, allowed %v: %v", publicIpId, protocol, publicPort, allowedIPs, err)
		}
	}

	// return true (because we changed something), but also the last error if deleting one old rule failed
	return true, err
}

func (lb *loadBalancer) updateNetworkACL(publicPort int, protocol LoadBalancerProtocol, networkId string) (bool, error) {
	network, _, err := lb.Network.GetNetworkByID(networkId, cloudstack.WithProject(lb.projectID))
	if err != nil {
		return false, fmt.Errorf("error fetching Network with ID: %v, due to: %s", networkId, err)
	}

	// Check if the network has an ACL ID - if not, skip ACL management
	if network.Aclid == "" {
		klog.V(4).Infof("Network %s does not have an ACL ID, skipping Network ACL management", networkId)
		return true, nil
	}

	networkAclList, count, err := lb.NetworkACL.GetNetworkACLListByID(network.Aclid, cloudstack.WithProject(lb.projectID))
	if err != nil {
		return false, fmt.Errorf("error fetching Network ACL List with ID: %v, due to: %s", network.Aclid, err)
	}

	if count == 0 {
		return false, fmt.Errorf("failed to find network ACL List with id: %v", network.Aclid)
	}

	if networkAclList.Name == "default_allow" || networkAclList.Name == "default_deny" {
		klog.Infof("Network is using a default network ACL. Cannot add ACL rules to default ACLs")
		return true, err
	}

	networkAclParams := lb.NetworkACL.NewListNetworkACLsParams()
	networkAclParams.SetAclid(network.Aclid)
	networkAclParams.SetNetworkid(networkId)

	networkAclResponse, err := lb.NetworkACL.ListNetworkACLs(networkAclParams)

	if err != nil {
		return false, fmt.Errorf("error fetching Network ACL with ID: %v for network with id: %v, due to: %s", network.Aclid, networkId, err)
	}

	// find all network ACL rules that have a matching proto+port
	// a map may or may not be faster, but is a bit easier to understand
	filtered := make(map[*cloudstack.NetworkACL]bool)
	for _, netAclRule := range networkAclResponse.NetworkACLs {
		if netAclRule.Protocol == protocol.IPProtocol() && netAclRule.Startport == strconv.Itoa(publicPort) && netAclRule.Endport == strconv.Itoa(publicPort) {
			filtered[netAclRule] = true
		}
	}

	if len(filtered) > 0 {
		klog.V(4).Infof("Network ACL rule for port %v and protocol %v already exists. No need to added a duplicate rule", publicPort, protocol)
		return true, err
	}

	// create ACL rule
	acl := lb.NetworkACL.NewCreateNetworkACLParams(protocol.CSProtocol())
	acl.SetAclid(network.Aclid)
	acl.SetAction("Allow")
	acl.SetCidrlist([]string{"0.0.0.0/0"})
	acl.SetStartport(publicPort)
	acl.SetEndport(publicPort)
	acl.SetNetworkid(networkId)
	acl.SetTraffictype("Ingress")

	_, err = lb.NetworkACL.CreateNetworkACL(acl)
	if err != nil {
		return false, fmt.Errorf("error creating Network ACL for port: %v, due to: %s", publicPort, err)
	}
	return true, err
}

// deleteFirewallRule deletes the firewall rule associated with the ip:port:protocol combo
//
// returns true when corresponding rules were deleted
func (lb *loadBalancer) deleteFirewallRule(publicIpId string, publicPort int, protocol LoadBalancerProtocol) (bool, error) {
	p := lb.Firewall.NewListFirewallRulesParams()
	p.SetIpaddressid(publicIpId)
	p.SetListall(true)
	if lb.projectID != "" {
		p.SetProjectid(lb.projectID)
	}
	r, err := lb.Firewall.ListFirewallRules(p)
	if err != nil {
		return false, fmt.Errorf("error fetching firewall rules for public IP %v: %v", publicIpId, err)
	}

	// filter by proto:port
	filtered := make([]*cloudstack.FirewallRule, 0, 1)
	for _, rule := range r.FirewallRules {
		if rule.Protocol == protocol.IPProtocol() && rule.Startport == publicPort && rule.Endport == publicPort {
			filtered = append(filtered, rule)
		}
	}

	// delete all rules
	deleted := false
	for _, rule := range filtered {
		p := lb.Firewall.NewDeleteFirewallRuleParams(rule.Id)
		_, err = lb.Firewall.DeleteFirewallRule(p)
		if err != nil {
			klog.Errorf("Error deleting old firewall rule %v: %v", rule.Id, err)
		} else {
			deleted = true
		}
	}

	return deleted, err
}

// Delete Network ACLs deletes the Network ACL rule associated with the ip:port:protocol combo
func (lb *loadBalancer) deleteNetworkACLRule(publicPort int, protocol LoadBalancerProtocol, networkID string) (bool, error) {
	// First check if the network has ACL support
	network, _, err := lb.Network.GetNetworkByID(networkID, cloudstack.WithProject(lb.projectID))
	if err != nil {
		klog.Errorf("Error fetching network %s for ACL rule deletion: %v", networkID, err)
		// Continue with deletion attempt even if network fetch fails
	} else if network.Aclid == "" {
		klog.V(4).Infof("Network %s does not have an ACL ID, skipping Network ACL rule deletion", networkID)
		return true, nil
	}

	p := lb.NetworkACL.NewListNetworkACLsParams()
	p.SetListall(true)
	p.SetNetworkid(networkID)
	if lb.projectID != "" {
		p.SetProjectid(lb.projectID)
	}

	r, err := lb.NetworkACL.ListNetworkACLs(p)
	if err != nil {
		return false, fmt.Errorf("error fetching Network ACL rules Network ID %v: %v", networkID, err)
	}

	// filter by proto:port
	filtered := make([]*cloudstack.NetworkACL, 0, 1)
	for _, rule := range r.NetworkACLs {
		if rule.Protocol == protocol.IPProtocol() && rule.Startport == strconv.Itoa(publicPort) && rule.Endport == strconv.Itoa(publicPort) {
			filtered = append(filtered, rule)
		}
	}

	// delete first filtered rules
	if len(filtered) == 0 {
		klog.V(4).Infof("No ACL rules found matching protocol: %v and port: %v", protocol, publicPort)
		return true, nil
	}
	deleted := false
	ruleToBeDeleted := filtered[0]
	deleteAclParams := lb.NetworkACL.NewDeleteNetworkACLParams(ruleToBeDeleted.Id)
	_, err = lb.NetworkACL.DeleteNetworkACL(deleteAclParams)
	if err != nil {
		klog.Errorf("Error deleting old Network ACL rule %v: %v", ruleToBeDeleted.Id, err)
	} else {
		deleted = true
	}

	return deleted, err
}

// getStringFromServiceAnnotation searches a given v1.Service for a specific annotationKey and either returns the annotation's value or a specified defaultSetting
func getStringFromServiceAnnotation(service *corev1.Service, annotationKey string, defaultSetting string) string {
	klog.V(4).Infof("getStringFromServiceAnnotation(%s/%s, %v, %v)", service.Namespace, service.Name, annotationKey, defaultSetting)
	if annotationValue, ok := service.Annotations[annotationKey]; ok {
		//if there is an annotation for this setting, set the "setting" var to it
		// annotationValue can be empty, it is working as designed
		// it makes possible for instance provisioning loadbalancer without floatingip
		klog.V(4).Infof("Found a Service Annotation: %v = %v", annotationKey, annotationValue)
		return annotationValue
	}
	//if there is no annotation, set "settings" var to the value from cloud config
	if defaultSetting != "" {
		klog.V(4).Infof("Could not find a Service Annotation; falling back on cloud-config setting: %v = %v", annotationKey, defaultSetting)
	}
	return defaultSetting
}

// getBoolFromServiceAnnotation searches a given v1.Service for a specific annotationKey and either returns the annotation's boolean value or a specified defaultSetting
func getBoolFromServiceAnnotation(service *corev1.Service, annotationKey string, defaultSetting bool) bool {
	klog.V(4).Infof("getBoolFromServiceAnnotation(%s/%s, %v, %v)", service.Namespace, service.Name, annotationKey, defaultSetting)
	if annotationValue, ok := service.Annotations[annotationKey]; ok {
		returnValue := false
		switch annotationValue {
		case "true":
			returnValue = true
		case "false":
			returnValue = false
		default:
			returnValue = defaultSetting
		}

		klog.V(4).Infof("Found a Service Annotation: %v = %v", annotationKey, returnValue)
		return returnValue
	}
	klog.V(4).Infof("Could not find a Service Annotation; falling back to default setting: %v = %v", annotationKey, defaultSetting)
	return defaultSetting
}

// filterNodesForExternalTrafficPolicy filters nodes based on the service's ExternalTrafficPolicy.
// When ExternalTrafficPolicy is Local, only nodes that are ready and have local endpoints should be used.
func (cs *CSCloud) filterNodesForExternalTrafficPolicy(service *corev1.Service, nodes []*corev1.Node) []*corev1.Node {
	// If ExternalTrafficPolicy is not Local, return all nodes
	if service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyTypeLocal {
		klog.V(4).Infof("Service %s/%s has ExternalTrafficPolicy: %v, using all nodes", service.Namespace, service.Name, service.Spec.ExternalTrafficPolicy)
		return nodes
	}

	klog.V(4).Infof("Service %s/%s has ExternalTrafficPolicy: Local, filtering nodes with local endpoints", service.Namespace, service.Name)

	// Get endpoints for this service to determine which nodes have local pods
	nodesWithEndpoints := cs.getNodesWithLocalEndpoints(service)
	if len(nodesWithEndpoints) == 0 {
		if cs.kubeClient == nil {
			klog.Warningf("Service %s/%s has ExternalTrafficPolicy: Local but kubeClient is not available", service.Namespace, service.Name)
			klog.Warningf("Falling back to using all nodes for service %s/%s", service.Namespace, service.Name)
			return nodes
		}
		klog.V(2).Infof("Service %s/%s has ExternalTrafficPolicy: Local but no local endpoints found - will create load balancer without members", service.Namespace, service.Name)
		return []*corev1.Node{}
	}

	var filteredNodes []*corev1.Node
	for _, node := range nodes {
		if cs.isNodeReadyForLocalTraffic(node) && nodesWithEndpoints[node.Name] {
			filteredNodes = append(filteredNodes, node)
		}
	}

	// If no nodes found but we have endpoints, check if endpoints are on control-plane nodes
	if len(filteredNodes) == 0 && cs.kubeClient != nil {
		klog.V(2).Infof("Service %s/%s: No worker nodes have local endpoints, checking for control-plane nodes with endpoints", service.Namespace, service.Name)
		controlPlaneNodes := cs.getControlPlaneNodesWithEndpoints(service, nodesWithEndpoints)
		if len(controlPlaneNodes) > 0 {
			klog.V(2).Infof("Service %s/%s: Automatically including %d control-plane nodes with local endpoints", service.Namespace, service.Name, len(controlPlaneNodes))
			filteredNodes = append(filteredNodes, controlPlaneNodes...)
		}
	}

	klog.V(4).Infof("Filtered %d out of %d nodes for local traffic policy (nodes with local endpoints: %d)", len(filteredNodes), len(nodes), len(nodesWithEndpoints))
	return filteredNodes
}

// isNodeReadyForLocalTraffic checks if a node is ready to receive traffic for services with Local external traffic policy.
// A node is considered ready if it has the Ready condition set to True.
// Note: We don't check if the node is schedulable (cordoned) because existing pods on cordoned nodes
// should still receive traffic for externalTrafficPolicy: Local.
func (cs *CSCloud) isNodeReadyForLocalTraffic(node *corev1.Node) bool {
	// Check if node is ready
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	// If no Ready condition found, assume not ready
	return false
}

// getNodesWithLocalEndpoints returns a map of node names that have local endpoints (running pods) for the given service.
// This is essential for externalTrafficPolicy: Local to ensure traffic only goes to nodes with local pods.
func (cs *CSCloud) getNodesWithLocalEndpoints(service *corev1.Service) map[string]bool {
	nodesWithEndpoints := make(map[string]bool)

	// If we don't have a Kubernetes client, we can't determine local endpoints
	if cs.kubeClient == nil {
		klog.Warningf("Kubernetes client not available, cannot determine local endpoints for service %s/%s", service.Namespace, service.Name)
		klog.Warningf("ExternalTrafficPolicy: Local will not work correctly without endpoint information")
		klog.Warningf("Falling back to using all nodes (equivalent to Cluster traffic policy)")
		return nodesWithEndpoints // Return empty map to trigger fallback logic
	}

	// Get endpoints for this service
	endpoints, err := cs.kubeClient.CoreV1().Endpoints(service.Namespace).Get(context.TODO(), service.Name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get endpoints for service %s/%s: %v", service.Namespace, service.Name, err)
		return nodesWithEndpoints
	}

	// Extract node names from endpoint addresses
	for _, subset := range endpoints.Subsets {
		// Check ready addresses (pods that are ready to receive traffic)
		for _, address := range subset.Addresses {
			if address.NodeName != nil {
				nodesWithEndpoints[*address.NodeName] = true
				klog.V(4).Infof("Found ready endpoint on node %s for service %s/%s", *address.NodeName, service.Namespace, service.Name)
			}
		}

		// We intentionally don't include NotReadyAddresses as those pods are not ready to receive traffic
		if len(subset.NotReadyAddresses) > 0 {
			klog.V(4).Infof("Service %s/%s has %d not-ready endpoints (these will be excluded)",
				service.Namespace, service.Name, len(subset.NotReadyAddresses))
		}
	}

	klog.V(4).Infof("Service %s/%s has ready endpoints on %d nodes: %v",
		service.Namespace, service.Name, len(nodesWithEndpoints), getMapKeys(nodesWithEndpoints))

	return nodesWithEndpoints
}

// getMapKeys returns the keys of a map as a slice (helper function for logging)
func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getControlPlaneNodesWithEndpoints fetches control-plane nodes from the cluster
// and returns only those that have local endpoints for the service.
func (cs *CSCloud) getControlPlaneNodesWithEndpoints(service *corev1.Service, nodesWithEndpoints map[string]bool) []*corev1.Node {
	// Fetch only nodes referenced by endpoints to minimize API calls
	var controlPlaneNodesWithEndpoints []*corev1.Node
	for nodeName := range nodesWithEndpoints {
		n, err := cs.kubeClient.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		if err != nil {
			klog.V(4).Infof("Service %s/%s: Could not fetch node %s: %v", service.Namespace, service.Name, nodeName, err)
			continue
		}

		if isControlPlaneNode(n) && cs.isNodeReadyForLocalTraffic(n) {
			// Create a local variable to take the address safely
			nodeCopy := n
			controlPlaneNodesWithEndpoints = append(controlPlaneNodesWithEndpoints, nodeCopy)
		}
	}
	return controlPlaneNodesWithEndpoints
}

func isControlPlaneNode(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	if _, ok := node.Labels["node-role.kubernetes.io/control-plane"]; ok {
		return true
	}
	if _, ok := node.Labels["node-role.kubernetes.io/master"]; ok {
		return true
	}
	return false
}

// validateLoadBalancerRuleNetwork ensures the rule's public IP is associated with the expected network
func (lb *loadBalancer) validateLoadBalancerRuleNetwork(lbRule *cloudstack.LoadBalancerRule) (bool, error) {
	if lbRule == nil {
		return false, fmt.Errorf("load balancer rule is nil")
	}

	if lbRule.Publicipid == "" {
		return false, fmt.Errorf("load balancer rule %s has no public IP ID", lbRule.Name)
	}

	ip, count, err := lb.Address.GetPublicIpAddressByID(lbRule.Publicipid, cloudstack.WithProject(lb.projectID))
	if err != nil || count == 0 {
		return false, fmt.Errorf("failed to retrieve public IP %s details: %v", lbRule.Publicipid, err)
	}

	if ip.Associatednetworkid == "" {
		return false, fmt.Errorf("public IP %s is not associated with any network", ip.Ipaddress)
	}

	if ip.Associatednetworkid != lb.networkID {
		return false, fmt.Errorf("rule is on network %s but expected %s", ip.Associatednetworkid, lb.networkID)
	}

	klog.V(4).Infof("validateLoadBalancerRuleNetwork: rule %s is correctly on network %s", lbRule.Name, lb.networkID)
	return true, nil
}
