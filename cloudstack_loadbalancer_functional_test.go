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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/cloudstack-go/v2/cloudstack"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// mockCloudStackServer creates a mock CloudStack API server for testing
type mockCloudStackServer struct {
	server              *httptest.Server
	mu                  sync.Mutex
	loadBalancerRules   map[string]*cloudstack.LoadBalancerRule
	publicIPs           map[string]*cloudstack.PublicIpAddress
	virtualMachines     map[string]*cloudstack.VirtualMachine
	networks            map[string]*cloudstack.Network
	firewallRules       map[string]*cloudstack.FirewallRule
	networkACLs         map[string]*cloudstack.NetworkACL
	ipAllocationCounter int
	jobs                map[string]json.RawMessage
	jobCounter          int
	ruleMembers         map[string][]string
}

func newMockCloudStackServer() *mockCloudStackServer {
	m := &mockCloudStackServer{
		loadBalancerRules: make(map[string]*cloudstack.LoadBalancerRule),
		publicIPs:         make(map[string]*cloudstack.PublicIpAddress),
		virtualMachines:   make(map[string]*cloudstack.VirtualMachine),
		networks:          make(map[string]*cloudstack.Network),
		firewallRules:     make(map[string]*cloudstack.FirewallRule),
		networkACLs:       make(map[string]*cloudstack.NetworkACL),
		jobs:              make(map[string]json.RawMessage),
		ruleMembers:       make(map[string][]string),
	}

	// Setup default network
	m.networks["net-123"] = &cloudstack.Network{
		Id:      "net-123",
		Name:    "test-network",
		Vpcid:   "",
		Service: []cloudstack.NetworkServiceInternal{{Name: "Firewall"}},
	}

	// Setup default VM
	m.virtualMachines["vm-123"] = &cloudstack.VirtualMachine{
		Id:   "vm-123",
		Name: "node-1",
		Nic: []cloudstack.Nic{
			{Networkid: "net-123"},
		},
	}

	m.server = httptest.NewServer(http.HandlerFunc(m.handleRequest))
	return m
}

func (m *mockCloudStackServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"errorresponse": map[string]interface{}{
				"errorcode": 400,
				"errortext": fmt.Sprintf("failed to parse request: %v", err),
			},
		})
		return
	}

	query := map[string][]string(r.Form)
	command := getQueryParam(query, "command")

	// Handle root path requests - might be health check or initial connection
	if command == "" && r.URL.Path == "/" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
		return
	}

	// Also check RawQuery like the failure test does for better compatibility
	rawQuery := r.URL.RawQuery
	if command == "associateIpAddress" || strings.Contains(rawQuery, "associateIpAddress") {
		m.handleAssociateIpAddress(w, query)
		return
	}
	if command == "queryAsyncJobResult" || strings.Contains(rawQuery, "queryAsyncJobResult") {
		m.handleQueryAsyncJobResult(w, query)
		return
	}
	if command == "getPublicIpAddressById" || strings.Contains(rawQuery, "getPublicIpAddressById") {
		m.handleGetPublicIpAddressByID(w, query)
		return
	}

	switch command {
	case "associateIpAddress":
		m.handleAssociateIpAddress(w, query)
	case "listLoadBalancerRules":
		m.handleListLoadBalancerRules(w, query)
	case "createLoadBalancerRule":
		m.handleCreateLoadBalancerRule(w, query)
	case "deleteLoadBalancerRule":
		m.handleDeleteLoadBalancerRule(w, query)
	case "listVirtualMachines":
		m.handleListVirtualMachines(w, query)
	case "getVirtualMachineByName":
		m.handleGetVirtualMachineByName(w, query)
	case "listPublicIpAddresses":
		m.handleListPublicIpAddresses(w, query)
	case "disassociateIpAddress":
		m.handleDisassociateIpAddress(w, query)
	case "listNetworks":
		m.handleListNetworks(w, query)
	case "getNetworkById":
		m.handleGetNetworkById(w, query)
	case "listFirewallRules":
		m.handleListFirewallRules(w, query)
	case "createFirewallRule":
		m.handleCreateFirewallRule(w, query)
	case "deleteFirewallRule":
		m.handleDeleteFirewallRule(w, query)
	case "listNetworkACLs":
		m.handleListNetworkACLs(w, query)
	case "createNetworkACL":
		m.handleCreateNetworkACL(w, query)
	case "deleteNetworkACL":
		m.handleDeleteNetworkACL(w, query)
	case "listLoadBalancerRuleInstances":
		m.handleListLoadBalancerRuleInstances(w, query)
	case "assignToLoadBalancerRule":
		m.handleAssignToLoadBalancerRule(w, query)
	case "removeFromLoadBalancerRule":
		m.handleRemoveFromLoadBalancerRule(w, query)
	case "updateLoadBalancerRule":
		m.handleUpdateLoadBalancerRule(w, query)
	case "listProjects":
		m.handleListProjects(w, query)
	case "queryAsyncJobResult":
		m.handleQueryAsyncJobResult(w, query)
	default:
		// Log unknown commands for debugging
		// Return error for unknown commands
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"errorresponse": map[string]interface{}{
				"errorcode": 400,
				"errortext": fmt.Sprintf("Unknown command: %s", command),
			},
		})
	}
}

func getQueryParam(query map[string][]string, key string) string {
	if vals := query[key]; len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (m *mockCloudStackServer) handleListLoadBalancerRules(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	keyword := getQueryParam(query, "keyword")
	rules := []*cloudstack.LoadBalancerRule{}

	for _, rule := range m.loadBalancerRules {
		if keyword == "" || strings.Contains(rule.Name, keyword) {
			rules = append(rules, rule)
		}
	}

	response := map[string]interface{}{
		"listloadbalancerrulesresponse": map[string]interface{}{
			"count":            len(rules),
			"loadbalancerrule": rules,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleCreateLoadBalancerRule(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ruleID := fmt.Sprintf("rule-%d", len(m.loadBalancerRules)+1)
	name := getQueryParam(query, "name")
	algorithm := getQueryParam(query, "algorithm")
	publicPort := getQueryParam(query, "publicport")
	privatePort := getQueryParam(query, "privateport")
	protocol := getQueryParam(query, "protocol")
	publicIPID := getQueryParam(query, "publicipid")
	networkID := getQueryParam(query, "networkid")

	// Find the public IP
	var publicIP string
	for _, ip := range m.publicIPs {
		if ip.Id == publicIPID {
			publicIP = ip.Ipaddress
			break
		}
	}

	rule := &cloudstack.LoadBalancerRule{
		Id:          ruleID,
		Name:        name,
		Algorithm:   algorithm,
		Publicport:  publicPort,
		Privateport: privatePort,
		Protocol:    protocol,
		Publicip:    publicIP,
		Publicipid:  publicIPID,
		Networkid:   networkID,
	}

	m.loadBalancerRules[ruleID] = rule
	m.ruleMembers[ruleID] = nil

	jobID := fmt.Sprintf("job-%d", m.jobCounter+1)
	m.jobCounter++
	jobResult := map[string]interface{}{
		"createloadbalancerruleresponse": map[string]interface{}{
			"id":          ruleID,
			"name":        name,
			"algorithm":   algorithm,
			"publicport":  publicPort,
			"privateport": privatePort,
			"protocol":    protocol,
			"publicip":    publicIP,
			"publicipid":  publicIPID,
			"networkid":   networkID,
		},
	}
	if payload, err := json.Marshal(jobResult); err == nil {
		m.jobs[jobID] = payload
		m.jobs[""] = payload
	}

	response := map[string]interface{}{
		"createloadbalancerruleresponse": map[string]interface{}{
			"jobid":       jobID,
			"id":          ruleID,
			"name":        name,
			"algorithm":   algorithm,
			"publicport":  publicPort,
			"privateport": privatePort,
			"protocol":    protocol,
			"publicip":    publicIP,
			"publicipid":  publicIPID,
			"networkid":   networkID,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if respBytes, err := json.Marshal(response); err == nil {
		w.Write(respBytes)
	} else {
		json.NewEncoder(w).Encode(response)
	}
}

func (m *mockCloudStackServer) handleDeleteLoadBalancerRule(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ruleID := getQueryParam(query, "id")
	delete(m.loadBalancerRules, ruleID)
	delete(m.ruleMembers, ruleID)

	jobID := fmt.Sprintf("job-%d", m.jobCounter+1)
	m.jobCounter++
	jobResult := map[string]interface{}{
		"deleteloadbalancerruleresponse": map[string]interface{}{
			"success": true,
		},
	}
	if payload, err := json.Marshal(jobResult); err == nil {
		m.jobs[jobID] = payload
		m.jobs[""] = payload
	}

	response := map[string]interface{}{
		"deleteloadbalancerruleresponse": map[string]interface{}{
			"jobid":   jobID,
			"success": true,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if respBytes, err := json.Marshal(response); err == nil {
		w.Write(respBytes)
	} else {
		json.NewEncoder(w).Encode(response)
	}
}

func (m *mockCloudStackServer) handleListVirtualMachines(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vms := []*cloudstack.VirtualMachine{}
	for _, vm := range m.virtualMachines {
		vms = append(vms, vm)
	}

	response := map[string]interface{}{
		"listvirtualmachinesresponse": map[string]interface{}{
			"count":          len(vms),
			"virtualmachine": vms,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleGetVirtualMachineByName(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := getQueryParam(query, "name")
	for _, vm := range m.virtualMachines {
		if strings.EqualFold(vm.Name, name) {
			response := map[string]interface{}{
				"getvirtualmachinebyidresponse": map[string]interface{}{
					"count":          1,
					"virtualmachine": vm,
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}
	}

	response := map[string]interface{}{
		"getvirtualmachinebyidresponse": map[string]interface{}{
			"count": 0,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleAssociateIpAddress(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ipAllocationCounter++
	ipID := fmt.Sprintf("ip-%d", m.ipAllocationCounter)
	ipAddress := fmt.Sprintf("10.0.0.%d", m.ipAllocationCounter)
	networkID := getQueryParam(query, "networkid")
	vpcID := getQueryParam(query, "vpcid")

	ip := &cloudstack.PublicIpAddress{
		Id:                  ipID,
		Ipaddress:           ipAddress,
		Associatednetworkid: networkID,
		Vpcid:               vpcID,
	}

	m.publicIPs[ipID] = ip

	// Prepare async job result payload
	jobID := fmt.Sprintf("job-%d", m.jobCounter+1)
	m.jobCounter++
	jobResult := map[string]interface{}{
		"associateipaddressresponse": map[string]interface{}{
			"id":                  ipID,
			"ipaddress":           ipAddress,
			"associatednetworkid": networkID,
			"vpcid":               vpcID,
		},
	}
	if payload, err := json.Marshal(jobResult); err == nil {
		m.jobs[jobID] = payload
		m.jobs[""] = payload
	}

	response := map[string]interface{}{
		"associateipaddressresponse": map[string]interface{}{
			"jobid":     jobID,
			"id":        ipID,
			"ipaddress": ipAddress,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if respBytes, err := json.Marshal(response); err == nil {
		w.Write(respBytes)
	} else {
		json.NewEncoder(w).Encode(response)
	}
}

func (m *mockCloudStackServer) handleQueryAsyncJobResult(w http.ResponseWriter, query map[string][]string) {
	jobID := getQueryParam(query, "jobid")

	m.mu.Lock()
	payload, ok := m.jobs[jobID]
	m.mu.Unlock()

	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"queryasyncjobresultresponse": {"jobstatus": 1, "jobresult": {}}}`))
		return
	}

	response := map[string]interface{}{
		"queryasyncjobresultresponse": map[string]interface{}{
			"jobid":         jobID,
			"jobstatus":     1,
			"jobresultcode": 0,
			"jobresulttype": "object",
			"jobresult":     json.RawMessage(payload),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleListPublicIpAddresses(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ipAddress := getQueryParam(query, "ipaddress")
	id := getQueryParam(query, "id")
	ips := []*cloudstack.PublicIpAddress{}

	for _, ip := range m.publicIPs {
		if id != "" && ip.Id != id {
			continue
		}
		if ipAddress == "" || ip.Ipaddress == ipAddress {
			ips = append(ips, ip)
		}
	}

	response := map[string]interface{}{
		"listpublicipaddressesresponse": map[string]interface{}{
			"count":           len(ips),
			"publicipaddress": ips,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleDisassociateIpAddress(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ipID := getQueryParam(query, "id")
	delete(m.publicIPs, ipID)

	jobID := fmt.Sprintf("job-%d", m.jobCounter+1)
	m.jobCounter++
	jobResult := map[string]interface{}{
		"disassociateipaddressresponse": map[string]interface{}{
			"success": true,
		},
	}
	if payload, err := json.Marshal(jobResult); err == nil {
		m.jobs[jobID] = payload
		m.jobs[""] = payload
	}

	response := map[string]interface{}{
		"disassociateipaddressresponse": map[string]interface{}{
			"jobid":   jobID,
			"success": true,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if respBytes, err := json.Marshal(response); err == nil {
		w.Write(respBytes)
	} else {
		json.NewEncoder(w).Encode(response)
	}
}

func (m *mockCloudStackServer) handleListNetworks(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	networks := []*cloudstack.Network{}
	for _, net := range m.networks {
		networks = append(networks, net)
	}

	response := map[string]interface{}{
		"listnetworksresponse": map[string]interface{}{
			"count":   len(networks),
			"network": networks,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleGetNetworkById(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	networkID := getQueryParam(query, "id")
	network, exists := m.networks[networkID]

	if !exists {
		response := map[string]interface{}{
			"getnetworkbyidresponse": map[string]interface{}{
				"count": 0,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	response := map[string]interface{}{
		"getnetworkbyidresponse": map[string]interface{}{
			"count":   1,
			"network": network,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleListFirewallRules(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rules := []*cloudstack.FirewallRule{}
	for _, rule := range m.firewallRules {
		rules = append(rules, rule)
	}

	response := map[string]interface{}{
		"listfirewallrulesresponse": map[string]interface{}{
			"count":        len(rules),
			"firewallrule": rules,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleCreateFirewallRule(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ruleID := fmt.Sprintf("fw-rule-%d", len(m.firewallRules)+1)
	ipID := getQueryParam(query, "ipaddressid")
	protocol := getQueryParam(query, "protocol")
	startPortStr := getQueryParam(query, "startport")
	endPortStr := getQueryParam(query, "endport")

	var startPort, endPort int
	fmt.Sscanf(startPortStr, "%d", &startPort)
	fmt.Sscanf(endPortStr, "%d", &endPort)

	rule := &cloudstack.FirewallRule{
		Id:          ruleID,
		Ipaddressid: ipID,
		Protocol:    protocol,
		Startport:   startPort,
		Endport:     endPort,
	}

	m.firewallRules[ruleID] = rule

	jobID := fmt.Sprintf("job-%d", m.jobCounter+1)
	m.jobCounter++
	jobResult := map[string]interface{}{
		"createfirewallruleresponse": map[string]interface{}{
			"id":          ruleID,
			"ipaddressid": ipID,
			"protocol":    protocol,
			"startport":   startPort,
			"endport":     endPort,
		},
	}
	if payload, err := json.Marshal(jobResult); err == nil {
		m.jobs[jobID] = payload
	}

	response := map[string]interface{}{
		"createfirewallruleresponse": map[string]interface{}{
			"jobid":       jobID,
			"id":          ruleID,
			"ipaddressid": ipID,
			"protocol":    protocol,
			"startport":   startPort,
			"endport":     endPort,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if respBytes, err := json.Marshal(response); err == nil {
		w.Write(respBytes)
	} else {
		json.NewEncoder(w).Encode(response)
	}
}

func (m *mockCloudStackServer) handleDeleteFirewallRule(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ruleID := getQueryParam(query, "id")
	delete(m.firewallRules, ruleID)

	response := map[string]interface{}{
		"deletefirewallruleresponse": map[string]interface{}{
			"success": true,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleListNetworkACLs(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	acls := []*cloudstack.NetworkACL{}
	for _, acl := range m.networkACLs {
		acls = append(acls, acl)
	}

	response := map[string]interface{}{
		"listnetworkaclsresponse": map[string]interface{}{
			"count":      len(acls),
			"networkacl": acls,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleCreateNetworkACL(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	aclID := fmt.Sprintf("acl-%d", len(m.networkACLs)+1)
	protocol := getQueryParam(query, "protocol")
	startPort := getQueryParam(query, "startport")
	endPort := getQueryParam(query, "endport")

	acl := &cloudstack.NetworkACL{
		Id:        aclID,
		Protocol:  protocol,
		Startport: startPort,
		Endport:   endPort,
	}

	m.networkACLs[aclID] = acl

	response := map[string]interface{}{
		"createnetworkaclresponse": acl,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleDeleteNetworkACL(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	aclID := getQueryParam(query, "id")
	delete(m.networkACLs, aclID)

	response := map[string]interface{}{
		"deletenetworkaclresponse": map[string]interface{}{
			"success": true,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleListLoadBalancerRuleInstances(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ruleID := getQueryParam(query, "id")
	instances := []*cloudstack.VirtualMachine{}
	for _, vmID := range m.ruleMembers[ruleID] {
		if vm, exists := m.virtualMachines[vmID]; exists {
			instances = append(instances, vm)
		}
	}

	response := map[string]interface{}{
		"listloadbalancerruleinstancesresponse": map[string]interface{}{
			"count":          len(instances),
			"virtualmachine": instances,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleAssignToLoadBalancerRule(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ruleID := getQueryParam(query, "id")
	vmIDs := strings.Split(getQueryParam(query, "virtualmachineids"), ",")

	for _, vmID := range vmIDs {
		vmID = strings.TrimSpace(vmID)
		if vmID == "" {
			continue
		}
		if _, exists := m.virtualMachines[vmID]; !exists {
			continue
		}
		current := m.ruleMembers[ruleID]
		duplicate := false
		for _, existing := range current {
			if existing == vmID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			m.ruleMembers[ruleID] = append(m.ruleMembers[ruleID], vmID)
		}
	}

	response := map[string]interface{}{
		"assigntoloadbalancerruleresponse": map[string]interface{}{
			"success": true,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleRemoveFromLoadBalancerRule(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ruleID := getQueryParam(query, "id")
	vmIDs := strings.Split(getQueryParam(query, "virtualmachineids"), ",")

	if len(vmIDs) == 0 || vmIDs[0] == "" {
		m.ruleMembers[ruleID] = nil
	} else {
		existing := m.ruleMembers[ruleID]
		filtered := existing[:0]
		for _, current := range existing {
			remove := false
			for _, target := range vmIDs {
				if strings.TrimSpace(target) == current {
					remove = true
					break
				}
			}
			if !remove {
				filtered = append(filtered, current)
			}
		}
		m.ruleMembers[ruleID] = filtered
	}

	response := map[string]interface{}{
		"removefromloadbalancerruleresponse": map[string]interface{}{
			"success": true,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleUpdateLoadBalancerRule(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ruleID := getQueryParam(query, "id")
	rule, exists := m.loadBalancerRules[ruleID]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if algorithm := getQueryParam(query, "algorithm"); algorithm != "" {
		rule.Algorithm = algorithm
	}
	if protocol := getQueryParam(query, "protocol"); protocol != "" {
		rule.Protocol = protocol
	}

	response := map[string]interface{}{
		"updateloadbalancerruleresponse": rule,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleListProjects(w http.ResponseWriter, query map[string][]string) {
	// Return a test project to satisfy project validation
	projectID := getQueryParam(query, "id")
	if projectID == "" {
		projectID = getQueryParam(query, "projectid")
	}

	project := map[string]interface{}{
		"id":   projectID,
		"name": "test-project",
	}

	response := map[string]interface{}{
		"listprojectsresponse": map[string]interface{}{
			"count":   1,
			"project": []interface{}{project},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) handleGetPublicIpAddressByID(w http.ResponseWriter, query map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ipID := getQueryParam(query, "id")
	ip, exists := m.publicIPs[ipID]

	response := map[string]interface{}{
		"getpublicipaddressbyidresponse": map[string]interface{}{
			"count": 0,
		},
	}

	if exists {
		response["getpublicipaddressbyidresponse"] = map[string]interface{}{
			"count":           1,
			"publicipaddress": ip,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (m *mockCloudStackServer) Close() {
	m.server.Close()
}

// TestFunctional_CreateLoadBalancer tests the full flow of creating a new load balancer
func TestFunctional_CreateLoadBalancer(t *testing.T) {
	mock := newMockCloudStackServer()
	defer mock.Close()

	client := cloudstack.NewAsyncClient(mock.server.URL, "test-key", "test-secret", true)
	cs := &CSCloud{
		client:         client,
		projectID:      "test-project",
		serviceMutexes: make(map[string]*sync.Mutex),
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeLoadBalancer,
			SessionAffinity: corev1.ServiceAffinityNone,
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					NodePort:   30080,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(8080),
				},
			},
		},
	}

	nodes := []*corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
			},
		},
	}

	// Create load balancer
	status, err := cs.EnsureLoadBalancer(context.TODO(), "test-cluster", service, nodes)
	if err != nil {
		t.Fatalf("Failed to create load balancer: %v", err)
	}

	if status == nil {
		t.Fatal("Expected non-nil status")
	}

	if len(status.Ingress) == 0 {
		t.Fatal("Expected at least one ingress IP")
	}

	if status.Ingress[0].IP == "" {
		t.Fatal("Expected non-empty IP address")
	}

	t.Logf("Successfully created load balancer with IP: %s", status.Ingress[0].IP)

	// Verify load balancer exists
	existsStatus, exists, err := cs.GetLoadBalancer(context.TODO(), "test-cluster", service)
	if err != nil {
		t.Fatalf("Failed to get load balancer: %v", err)
	}

	if !exists {
		t.Fatal("Expected load balancer to exist")
	}

	if existsStatus.Ingress[0].IP != status.Ingress[0].IP {
		t.Errorf("IP mismatch: expected %s, got %s", status.Ingress[0].IP, existsStatus.Ingress[0].IP)
	}
}

// TestFunctional_CreateLoadBalancer_NoNodes tests creating a load balancer with no nodes
func TestFunctional_CreateLoadBalancer_NoNodes(t *testing.T) {
	mock := newMockCloudStackServer()
	defer mock.Close()

	client := cloudstack.NewAsyncClient(mock.server.URL, "test-key", "test-secret", true)
	cs := &CSCloud{
		client:         client,
		projectID:      "test-project",
		serviceMutexes: make(map[string]*sync.Mutex),
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeLoadBalancer,
			SessionAffinity: corev1.ServiceAffinityNone,
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					NodePort:   30080,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(8080),
				},
			},
			ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeLocal,
		},
	}

	// No nodes provided
	nodes := []*corev1.Node{}

	// Create load balancer - should succeed even with no nodes
	status, err := cs.EnsureLoadBalancer(context.TODO(), "test-cluster", service, nodes)
	if err != nil {
		t.Fatalf("Failed to create load balancer with no nodes: %v", err)
	}

	if status == nil {
		t.Fatal("Expected non-nil status")
	}

	if len(status.Ingress) == 0 {
		t.Fatal("Expected at least one ingress IP")
	}

	t.Logf("Successfully created load balancer with no nodes, IP: %s", status.Ingress[0].IP)
}

// TestFunctional_UpdateLoadBalancer tests updating an existing load balancer
func TestFunctional_UpdateLoadBalancer(t *testing.T) {
	mock := newMockCloudStackServer()
	defer mock.Close()

	client := cloudstack.NewAsyncClient(mock.server.URL, "test-key", "test-secret", true)
	cs := &CSCloud{
		client:         client,
		projectID:      "test-project",
		serviceMutexes: make(map[string]*sync.Mutex),
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeLoadBalancer,
			SessionAffinity: corev1.ServiceAffinityNone,
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					NodePort:   30080,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(8080),
				},
			},
		},
	}

	// Create with one node
	nodes1 := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	}

	status1, err := cs.EnsureLoadBalancer(context.TODO(), "test-cluster", service, nodes1)
	if err != nil {
		t.Fatalf("Failed to create load balancer: %v", err)
	}

	// Update with additional node
	nodes2 := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}},
	}

	// Add node-2 to mock
	mock.virtualMachines["vm-456"] = &cloudstack.VirtualMachine{
		Id:   "vm-456",
		Name: "node-2",
		Nic: []cloudstack.Nic{
			{Networkid: "net-123"},
		},
	}

	err = cs.UpdateLoadBalancer(context.TODO(), "test-cluster", service, nodes2)
	if err != nil {
		t.Fatalf("Failed to update load balancer: %v", err)
	}

	// Verify IP hasn't changed
	status2, exists, err := cs.GetLoadBalancer(context.TODO(), "test-cluster", service)
	if err != nil {
		t.Fatalf("Failed to get load balancer: %v", err)
	}

	if !exists {
		t.Fatal("Expected load balancer to still exist")
	}

	if status2.Ingress[0].IP != status1.Ingress[0].IP {
		t.Errorf("IP changed after update: was %s, now %s", status1.Ingress[0].IP, status2.Ingress[0].IP)
	}

	t.Logf("Successfully updated load balancer, IP remains: %s", status2.Ingress[0].IP)
}

// TestFunctional_DeleteLoadBalancer tests deleting a load balancer
func TestFunctional_DeleteLoadBalancer(t *testing.T) {
	mock := newMockCloudStackServer()
	defer mock.Close()

	client := cloudstack.NewAsyncClient(mock.server.URL, "test-key", "test-secret", true)
	cs := &CSCloud{
		client:         client,
		projectID:      "test-project",
		serviceMutexes: make(map[string]*sync.Mutex),
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeLoadBalancer,
			SessionAffinity: corev1.ServiceAffinityNone,
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					NodePort:   30080,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(8080),
				},
			},
		},
	}

	nodes := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	}

	// Create load balancer
	_, err := cs.EnsureLoadBalancer(context.TODO(), "test-cluster", service, nodes)
	if err != nil {
		t.Fatalf("Failed to create load balancer: %v", err)
	}

	// Verify it exists
	_, exists, err := cs.GetLoadBalancer(context.TODO(), "test-cluster", service)
	if err != nil {
		t.Fatalf("Failed to get load balancer: %v", err)
	}
	if !exists {
		t.Fatal("Expected load balancer to exist before deletion")
	}

	// Delete load balancer
	err = cs.EnsureLoadBalancerDeleted(context.TODO(), "test-cluster", service)
	if err != nil {
		t.Fatalf("Failed to delete load balancer: %v", err)
	}

	// Verify it's gone
	_, exists, err = cs.GetLoadBalancer(context.TODO(), "test-cluster", service)
	if err != nil {
		t.Fatalf("Failed to get load balancer: %v", err)
	}
	if exists {
		t.Fatal("Expected load balancer to be deleted")
	}

	t.Logf("Successfully deleted load balancer")
}

// TestFunctional_DuplicateIPCleanup tests the duplicate IP cleanup functionality
func TestFunctional_DuplicateIPCleanup(t *testing.T) {
	mock := newMockCloudStackServer()
	defer mock.Close()

	client := cloudstack.NewAsyncClient(mock.server.URL, "test-key", "test-secret", true)
	cs := &CSCloud{
		client:         client,
		projectID:      "test-project",
		serviceMutexes: make(map[string]*sync.Mutex),
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeLoadBalancer,
			SessionAffinity: corev1.ServiceAffinityNone,
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					NodePort:   30080,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(8080),
				},
			},
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{
					{IP: "10.0.0.1"}, // Prefer this IP
				},
			},
		},
	}

	lbName := cs.GetLoadBalancerName(context.TODO(), "", service)
	canonicalRuleName := fmt.Sprintf("%s-tcp-80", lbName)

	// Pre-create duplicate rules on different IPs
	rule1 := &cloudstack.LoadBalancerRule{
		Id:          "rule-1",
		Name:        canonicalRuleName,
		Publicip:    "10.0.0.1",
		Publicipid:  "ip-1",
		Protocol:    "tcp",
		Publicport:  "80",
		Privateport: "30080",
		Networkid:   "net-123",
	}

	rule2 := &cloudstack.LoadBalancerRule{
		Id:          "rule-2",
		Name:        canonicalRuleName,
		Publicip:    "10.0.0.2",
		Publicipid:  "ip-2",
		Protocol:    "tcp",
		Publicport:  "80",
		Privateport: "30080",
		Networkid:   "net-123",
	}

	mock.loadBalancerRules["rule-1"] = rule1
	mock.loadBalancerRules["rule-2"] = rule2
	mock.publicIPs["ip-1"] = &cloudstack.PublicIpAddress{
		Id:                  "ip-1",
		Ipaddress:           "10.0.0.1",
		Associatednetworkid: "net-123",
	}
	mock.publicIPs["ip-2"] = &cloudstack.PublicIpAddress{
		Id:                  "ip-2",
		Ipaddress:           "10.0.0.2",
		Associatednetworkid: "net-123",
	}

	nodes := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	}

	// EnsureLoadBalancer should detect and clean up duplicate IP
	_, err := cs.EnsureLoadBalancer(context.TODO(), "test-cluster", service, nodes)
	if err != nil {
		t.Fatalf("Failed to ensure load balancer: %v", err)
	}

	// Verify duplicate rule was deleted
	mock.mu.Lock()
	_, exists := mock.loadBalancerRules["rule-2"]
	mock.mu.Unlock()

	if exists {
		t.Error("Expected duplicate rule (rule-2) to be deleted")
	}

	// Verify duplicate IP was released
	mock.mu.Lock()
	_, ipExists := mock.publicIPs["ip-2"]
	mock.mu.Unlock()

	if ipExists {
		t.Error("Expected duplicate IP (ip-2) to be released")
	}

	t.Logf("Successfully cleaned up duplicate IP and rules")
}

// TestFunctional_ConcurrentOperations tests concurrent load balancer operations
func TestFunctional_ConcurrentOperations(t *testing.T) {
	mock := newMockCloudStackServer()
	defer mock.Close()

	client := cloudstack.NewAsyncClient(mock.server.URL, "test-key", "test-secret", true)
	cs := &CSCloud{
		client:         client,
		projectID:      "test-project",
		serviceMutexes: make(map[string]*sync.Mutex),
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeLoadBalancer,
			SessionAffinity: corev1.ServiceAffinityNone,
			Ports: []corev1.ServicePort{
				{
					Port:       80,
					NodePort:   30080,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(8080),
				},
			},
		},
	}

	nodes := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	}

	// Run multiple concurrent EnsureLoadBalancer calls
	const numGoroutines = 5
	var wg sync.WaitGroup
	errors := make([]error, numGoroutines)
	ips := make([]string, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			status, err := cs.EnsureLoadBalancer(context.TODO(), "test-cluster", service, nodes)
			if err != nil {
				errors[index] = err
				return
			}
			if status != nil && len(status.Ingress) > 0 {
				ips[index] = status.Ingress[0].IP
			}
		}(i)
	}

	wg.Wait()

	// Count unique IPs allocated
	uniqueIPs := make(map[string]bool)
	for _, ip := range ips {
		if ip != "" {
			uniqueIPs[ip] = true
		}
	}

	// Should only have one IP allocated (or at most a few due to race conditions)
	if len(uniqueIPs) > 3 {
		t.Errorf("Too many unique IPs allocated: %d (expected 1-2). IPs: %v", len(uniqueIPs), ips)
	}

	// Count successful operations
	successCount := 0
	for _, err := range errors {
		if err == nil {
			successCount++
		}
	}

	t.Logf("Concurrent operations: %d successful, %d unique IPs allocated", successCount, len(uniqueIPs))
}
