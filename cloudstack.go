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
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/cloudstack-go/v2/cloudstack"
	"gopkg.in/gcfg.v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

// ProviderName is the name of this cloud provider.
const ProviderName = "external-cloudstack"

// CSConfig wraps the config for the CloudStack cloud provider.
type CSConfig struct {
	Global struct {
		APIURL      string `gcfg:"api-url"`
		APIKey      string `gcfg:"api-key"`
		SecretKey   string `gcfg:"secret-key"`
		SSLNoVerify bool   `gcfg:"ssl-no-verify"`
		ProjectID   string `gcfg:"project-id"`
		Zone        string `gcfg:"zone"`
	}
}

// CSCloud is an implementation of Interface for CloudStack.
type CSCloud struct {
	client     *cloudstack.CloudStackClient
	kubeClient kubernetes.Interface
	projectID  string // If non-"", all resources will be created within this project
	zone       string
}

func init() {
	cloudprovider.RegisterCloudProvider(ProviderName, func(config io.Reader) (cloudprovider.Interface, error) {
		cfg, err := readConfig(config)
		if err != nil {
			return nil, err
		}

		return newCSCloud(cfg)
	})
}

func readConfig(config io.Reader) (*CSConfig, error) {
	cfg := &CSConfig{}

	if config == nil {
		return cfg, nil
	}

	if err := gcfg.ReadInto(cfg, config); err != nil {
		return nil, fmt.Errorf("could not parse cloud provider config: %v", err)
	}

	return cfg, nil
}

// newCSCloud creates a new instance of CSCloud.
func newCSCloud(cfg *CSConfig) (*CSCloud, error) {
	cs := &CSCloud{
		projectID: cfg.Global.ProjectID,
		zone:      cfg.Global.Zone,
	}

	if cfg.Global.APIURL != "" && cfg.Global.APIKey != "" && cfg.Global.SecretKey != "" {
		cs.client = cloudstack.NewAsyncClient(cfg.Global.APIURL, cfg.Global.APIKey, cfg.Global.SecretKey, !cfg.Global.SSLNoVerify)
	}

	if cs.client == nil {
		return nil, errors.New("no cloud provider config given")
	}

	return cs, nil
}

// Initialize passes a Kubernetes clientBuilder interface to the cloud provider
func (cs *CSCloud) Initialize(clientBuilder cloudprovider.ControllerClientBuilder, stop <-chan struct{}) {
	if clientBuilder != nil {
		kubeClient, err := clientBuilder.Client("")
		if err != nil {
			klog.Warningf("Failed to create Kubernetes client: %v", err)
		} else {
			cs.kubeClient = kubeClient
			klog.V(2).Infof("Successfully initialized Kubernetes client for endpoint-aware load balancing")

			// Start endpoint watcher for automatic load balancer updates
			klog.V(2).Infof("Starting endpoint watcher goroutine")
			go cs.startEndpointWatcher(stop)
		}
	} else {
		klog.Warningf("No clientBuilder provided - endpoint watcher will not start")
	}
}

// LoadBalancer returns an implementation of LoadBalancer for CloudStack.
func (cs *CSCloud) LoadBalancer() (cloudprovider.LoadBalancer, bool) {
	if cs.client == nil {
		return nil, false
	}

	return cs, true
}

// Instances returns an implementation of Instances for CloudStack.
func (cs *CSCloud) Instances() (cloudprovider.Instances, bool) {
	if cs.client == nil {
		return nil, false
	}

	return cs, true
}

func (cs *CSCloud) InstancesV2() (cloudprovider.InstancesV2, bool) {
	if cs.client == nil {
		return nil, false
	}

	return cs, true
}

// Zones returns an implementation of Zones for CloudStack.
func (cs *CSCloud) Zones() (cloudprovider.Zones, bool) {
	if cs.client == nil {
		return nil, false
	}

	return cs, true
}

// Clusters returns an implementation of Clusters for CloudStack.
func (cs *CSCloud) Clusters() (cloudprovider.Clusters, bool) {
	if cs.client == nil {
		return nil, false
	}

	klog.Warning("This cloud provider doesn't support clusters")
	return nil, false
}

// Routes returns an implementation of Routes for CloudStack.
func (cs *CSCloud) Routes() (cloudprovider.Routes, bool) {
	if cs.client == nil {
		return nil, false
	}

	klog.Warning("This cloud provider doesn't support routes")
	return nil, false
}

// ProviderName returns the cloud provider ID.
func (cs *CSCloud) ProviderName() string {
	return ProviderName
}

// HasClusterID returns true if the cluster has a clusterID
func (cs *CSCloud) HasClusterID() bool {
	return true
}

// GetZone returns the Zone containing the region that the program is running in.
func (cs *CSCloud) GetZone(ctx context.Context) (cloudprovider.Zone, error) {
	zone := cloudprovider.Zone{}

	if cs.zone == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return zone, fmt.Errorf("failed to get hostname for retrieving the zone: %v", err)
		}

		instance, count, err := cs.client.VirtualMachine.GetVirtualMachineByName(hostname)
		if err != nil {
			if count == 0 {
				return zone, fmt.Errorf("could not find instance for retrieving the zone: %v", err)
			}
			return zone, fmt.Errorf("error getting instance for retrieving the zone: %v", err)
		}

		cs.zone = instance.Zonename
	}

	klog.V(2).Infof("Current zone is %v", cs.zone)
	zone.FailureDomain = cs.zone
	zone.Region = cs.zone

	return zone, nil
}

// GetZoneByProviderID returns the Zone, found by using the provider ID.
func (cs *CSCloud) GetZoneByProviderID(ctx context.Context, providerID string) (cloudprovider.Zone, error) {
	zone := cloudprovider.Zone{}

	instance, count, err := cs.client.VirtualMachine.GetVirtualMachineByID(
		providerID,
		cloudstack.WithProject(cs.projectID),
	)
	if err != nil {
		if count == 0 {
			return zone, fmt.Errorf("could not find node by ID: %v", providerID)
		}
		return zone, fmt.Errorf("error retrieving zone: %v", err)
	}

	klog.V(2).Infof("Current zone is %v", cs.zone)
	zone.FailureDomain = instance.Zonename
	zone.Region = instance.Zonename

	return zone, nil
}

// GetZoneByNodeName returns the Zone, found by using the node name.
func (cs *CSCloud) GetZoneByNodeName(ctx context.Context, nodeName types.NodeName) (cloudprovider.Zone, error) {
	zone := cloudprovider.Zone{}

	instance, count, err := cs.client.VirtualMachine.GetVirtualMachineByName(
		string(nodeName),
		cloudstack.WithProject(cs.projectID),
	)
	if err != nil {
		if count == 0 {
			return zone, fmt.Errorf("could not find node: %v", nodeName)
		}
		return zone, fmt.Errorf("error retrieving zone: %v", err)
	}

	klog.V(2).Infof("Current zone is %v", cs.zone)
	zone.FailureDomain = instance.Zonename
	zone.Region = instance.Zonename

	return zone, nil
}

// startEndpointWatcher watches for endpoint changes and automatically updates load balancers
// for services with externalTrafficPolicy: Local
func (cs *CSCloud) startEndpointWatcher(stop <-chan struct{}) {
	klog.V(2).Infof("Starting endpoint watcher for automatic load balancer updates")

	// Create a map to debounce updates (avoid too frequent updates for the same service)
	pendingUpdates := make(map[string]*time.Timer)

	// Watch all endpoints
	watchlist := &metav1.ListOptions{
		FieldSelector: fields.Everything().String(),
	}

	for {
		select {
		case <-stop:
			klog.V(2).Infof("Stopping endpoint watcher")
			return
		default:
		}

		watcher, err := cs.kubeClient.CoreV1().Endpoints("").Watch(context.TODO(), *watchlist)
		if err != nil {
			klog.Errorf("Failed to watch endpoints: %v", err)
			time.Sleep(30 * time.Second)
			continue
		}

		klog.V(2).Infof("Endpoint watcher connected successfully, watching for changes")

	watchLoop:
		for {
			select {
			case <-stop:
				watcher.Stop()
				klog.V(2).Infof("Stopping endpoint watcher")
				return
			case event, ok := <-watcher.ResultChan():
				if !ok {
					klog.V(4).Infof("Endpoint watcher disconnected, reconnecting")
					break watchLoop
				}

				if event.Type == watch.Modified || event.Type == watch.Added {
					if endpoints, ok := event.Object.(*corev1.Endpoints); ok {
						klog.V(4).Infof("Received endpoint event: %s for %s/%s", event.Type, endpoints.Namespace, endpoints.Name)
						cs.handleEndpointUpdate(endpoints, pendingUpdates)
					}
				}
			}
		}

		watcher.Stop()
		time.Sleep(5 * time.Second) // Wait before reconnecting
	}
}

// handleEndpointUpdate processes endpoint changes and triggers load balancer updates if needed
func (cs *CSCloud) handleEndpointUpdate(endpoints *corev1.Endpoints, pendingUpdates map[string]*time.Timer) {
	serviceKey := endpoints.Namespace + "/" + endpoints.Name

	klog.V(4).Infof("Processing endpoint update for service %s", serviceKey)

	// Get the corresponding service
	service, err := cs.kubeClient.CoreV1().Services(endpoints.Namespace).Get(context.TODO(), endpoints.Name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Could not find service %s: %v", serviceKey, err)
		return
	}

	klog.V(4).Infof("Found service %s: Type=%s, ExternalTrafficPolicy=%s", serviceKey, service.Spec.Type, service.Spec.ExternalTrafficPolicy)

	// Only process LoadBalancer services with Local external traffic policy
	if service.Spec.Type != corev1.ServiceTypeLoadBalancer {
		klog.V(4).Infof("Ignoring service %s: not a LoadBalancer (Type=%s)", serviceKey, service.Spec.Type)
		return
	}

	if service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyTypeLocal {
		klog.V(4).Infof("Ignoring service %s: not Local traffic policy (ExternalTrafficPolicy=%s)",
			serviceKey, service.Spec.ExternalTrafficPolicy)
		return
	}

	klog.V(2).Infof("Endpoint change detected for LoadBalancer service %s with Local traffic policy - scheduling update", serviceKey)

	// Cancel any existing pending update for this service
	if timer, exists := pendingUpdates[serviceKey]; exists {
		klog.V(4).Infof("Cancelling previous pending update for service %s", serviceKey)
		timer.Stop()
		delete(pendingUpdates, serviceKey)
	}

	// Schedule a debounced update (wait 10 seconds for more changes)
	klog.V(4).Infof("Scheduling debounced update for service %s (10s delay)", serviceKey)
	pendingUpdates[serviceKey] = time.AfterFunc(10*time.Second, func() {
		delete(pendingUpdates, serviceKey)
		cs.updateLoadBalancerForEndpointChange(service)
	})
}

// updateLoadBalancerForEndpointChange triggers EnsureLoadBalancer for a service whose endpoints changed
func (cs *CSCloud) updateLoadBalancerForEndpointChange(service *corev1.Service) {
	klog.V(2).Infof("Triggering load balancer update for service %s/%s due to endpoint changes",
		service.Namespace, service.Name)

	// Get all cluster nodes
	nodes, err := cs.kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to list nodes for load balancer update: %v", err)
		return
	}

	klog.V(4).Infof("Retrieved %d cluster nodes for load balancer update", len(nodes.Items))

	// Convert to []*corev1.Node
	nodePointers := make([]*corev1.Node, len(nodes.Items))
	for i := range nodes.Items {
		nodePointers[i] = &nodes.Items[i]
	}

	// Call EnsureLoadBalancer to update the load balancer
	_, err = cs.EnsureLoadBalancer(context.TODO(), "", service, nodePointers)
	if err != nil {
		klog.Errorf("Failed to update load balancer for service %s/%s: %v",
			service.Namespace, service.Name, err)
	} else {
		klog.V(2).Infof("Successfully updated load balancer for service %s/%s due to endpoint changes",
			service.Namespace, service.Name)
	}
}
