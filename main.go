// metaminer
// retrieves information about a virtual machine running in azure from the azure metadata service
// extracts related information about the virtual machine from a puppetdb service running inside kubernets
// resulting metadata is parsed and merged with any existing records in an adjacent redis cluster

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/compute/mgmt/compute"
	"github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/network/mgmt/network"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/resources/mgmt/resources"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

const (
	pdb_uri = "http://puppetdb.puppetserver.svc.cluster.local:8080/pdb/query/v4"
)

var vmObjectList []vmData

type puppetMetadata struct {
	ConfigurationState string                 `json:"configurationState"`
	BuildVersion       string                 `json:"buildVersion"`
	LastPuppetRun      string                 `json:"lastPuppetRun"`
	PuppetEvents       map[string]interface{} `json:"puppetEvents"`
}

type providerMetadata struct {
	Name              string            `json:"name"`
	VmID              string            `json:"vmid"`
	VmType            string            `json:"vmType"`
	ProvisioningState string            `json:"provisioningState"`
	PowerState        string            `json:"powerState"`
	IpAddress         string            `json:"ipAddress"`
	OperatingSystem   string            `json:"operatingSystem"`
	DiskSize          int32             `json:"diskSize"`
	ResourceGroup     string            `json:"resourceGroup"`
	VmImage           map[string]string `json:"vmImage"`
	Tags              map[string]string `json:"tags"`
}

type foundationMetadata struct {
	Hostname          string `json:"hostname"`
	Provider          string `json:"provider"`
	AvailabilityZone  string `json:"availabilityZone"`
	Platform          string `json:"platform"`
	ServiceUptime     string `json:"serviceUptime"`
	ShowName          string `json:"showName"`
	NiceName          string `json:"niceName"`
	ServiceDomain     string `json:"serviceDomain"`
	LocalDomain       string `json:"localDomain"`
	Region            string `json:"region"`
	Heartbeat         string `json:"heartbeat"`
	Distro            string `json:"distro"`
	OsVendor          string `json:"osVendor"`
	MajVer            string `json:"majVer"`
	VaultRole         string `json:"vaultRole"`
	PuppetSchedule    string `json:"puppetSchedule"`
	BootstrapVersion  string `json:"bootstrapVersion"`
	FoundationVersion string `json:"foundationVersion"`
	PipelineId        string `json:"pipelineId"`
}

type vmData struct {
	ProviderMetadata   providerMetadata   `json:"providerMetadata"`
	PuppetMetadata     puppetMetadata     `json:"puppetMetadata"`
	FoundationMetadata foundationMetadata `json:"foundationMetadata"`
}

func postRedis(key string, payload []byte) error {
	var ctx = context.Background()
	rdb := redis.NewFailoverClusterClient(&redis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"primary-redis.redis.svc.cluster.local:26379"},
		RouteRandomly: true,
		Password:      "",
		DB:            0,
	})

	// check whether key exists
	// if so, load the values into vmData struct and override payload var with merged record
	exists, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		return err
	} else if exists == 1 {
		println(key, "exists, will retrieve and update record")
		var existingRecord, payloadRecord vmData
		val, err := rdb.Get(ctx, key).Bytes()
		if err != nil {
			return err
		} else {
			json.Unmarshal(val, &existingRecord)
			json.Unmarshal(payload, &payloadRecord)
			existingRecord.ProviderMetadata = payloadRecord.ProviderMetadata
			existingRecord.PuppetMetadata = payloadRecord.PuppetMetadata
			payload, err = json.Marshal(existingRecord)
		}
	}

	err = rdb.Set(ctx, key, payload, 24*time.Hour).Err()
	if err != nil {
		return err
	}

	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return err
	} else {
		println(val)
	}

	return nil
}

func getAzResourceGroups(subscriptionId string, authorizer autorest.Authorizer, ctx context.Context) []string {
	groupsClient := resources.NewGroupsClient(subscriptionId)
	groupsClient.Authorizer = authorizer

	groups, err := groupsClient.ListComplete(ctx, "", nil)
	if err != nil {
		log.Println("Error getting groups", err)
	}

	var groupList []string
	for groups.NotDone() {
		groupList = append(groupList, *groups.Value().Name)
		err := groups.NextWithContext(ctx)
		if err != nil {
			log.Println("error getting next group")
		}
	}

	return groupList
}

func (v vmData) Compose() {
	vmObjectList = append(vmObjectList, v)
}

func getIpAddress(subscriptionId string, rg string, authorizer autorest.Authorizer, ctx context.Context, instanceMetadata compute.VirtualMachine) string {
	var interfaceId string

	interfaceClient := network.NewInterfacesClient(subscriptionId)
	interfaceClient.Authorizer = authorizer

	interfaces := instanceMetadata.NetworkProfile.NetworkInterfaces
	for _, v := range *interfaces {
		interfaceId = *v.ID
	}
	interfaceSplit := strings.Split(interfaceId, "/")
	interfaceName := interfaceSplit[len(interfaceSplit)-1]

	interfaceData, err := interfaceClient.Get(ctx, rg, interfaceName, "")
	if err != nil {
		log.Printf("%v", err)
	}

	var ipConfigName string
	ipConfig := *interfaceData.IPConfigurations
	for _, v := range ipConfig {
		ipConfigName = *v.Name
	}

	ipAddress := "unknown"
	netClient := network.NewInterfaceIPConfigurationsClient(subscriptionId)
	netClient.Authorizer = authorizer
	ipConfigData, err := netClient.Get(ctx, rg, interfaceName, ipConfigName)
	if err != nil {
		log.Printf("%v", err)
	} else {
		ipAddress = *ipConfigData.PrivateIPAddress
	}

	return ipAddress
}

func getImageData(instanceMetadata compute.VirtualMachine) map[string]string {
	offer, publisher, sku, version := "unknown", "unknown", "unknown", "unknown"

	vmImage := instanceMetadata.VirtualMachineProperties.StorageProfile.ImageReference
	if vmImage.Offer != nil && vmImage.Publisher != nil && vmImage.Sku != nil && vmImage.Version != nil {
		offer = *vmImage.Offer
		publisher = *vmImage.Publisher
		sku = *vmImage.Offer
		version = *vmImage.Offer
	}

	imageData := map[string]string{
		"offer":     offer,
		"publisher": publisher,
		"sku":       sku,
		"version":   version,
	}

	return imageData
}

func getDiskSize(instanceMetadata compute.VirtualMachine) int32 {
	diskData := *instanceMetadata.StorageProfile.OsDisk
	if diskData.DiskSizeGB != nil {
		return *diskData.DiskSizeGB
	}

	return 0
}

func parsePuppetData(tagsList map[string]string, vmID string) (string, string, string, map[string]interface{}) {
	var configurationState string
	var buildVersion string
	var lastPuppetRun string
	var lastReportHash string
	puppetEvents := make(map[string]interface{})

	if tagsList["deploymentUid"] != "" {
		pdbCheck := getPdbHostname(tagsList["deploymentUid"], vmID)
		for k, v := range pdbCheck {
			fmt.Println(k, "=>", v)
		}
		pdbNodeData := getPdbNodeData(tagsList["deploymentUid"], vmID)
		for _, x := range pdbNodeData {
			for k, v := range x {
				if k == "catalog_environment" {
					buildVersion = v.(string)
				} else if k == "report_timestamp" {
					lastPuppetRun = v.(string)
				} else if k == "latest_report_status" {
					configurationState = v.(string)
				}
			}
		}
		pdbReportData := getPdbReport(tagsList["deploymentUid"], vmID)
		for _, x := range pdbReportData {
			for k, v := range x {
				if k == "hash" {
					lastReportHash = v.(string)
				}
				latestRunEvents := getPdbEvents(lastReportHash)
				for _, x := range latestRunEvents {
					for k, v := range x {
						if k == "status" {
							puppetEvents["status"] = v.(string)
						} else if k == "resource_title" {
							puppetEvents["resource"] = v.(string)
						} else if k == "message" {
							puppetEvents["message"] = v.(string)
						} else if k == "file" {
							puppetEvents["file"] = v.(string)
						} else if k == "line" {
							puppetEvents["line"] = v.(float64)
						}
					}
				}
			}
		}
	} else {
		configurationState = "pending"
	}

	return configurationState, buildVersion, lastPuppetRun, puppetEvents
}

func getAzVms(subscriptionId string, rg string, authorizer autorest.Authorizer, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	vmClient := compute.NewVirtualMachinesClient(subscriptionId)
	vmClient.Authorizer = authorizer

	for vm, _ := vmClient.ListComplete(ctx, rg); vm.NotDone(); vm.Next() {
		i := vm.Value()
		vmID := *vm.Value().VMID

		instanceMetadata, err := vmClient.Get(ctx, rg, *i.Name, "")
		if err != nil {
			log.Printf("%v", err)
		}

		instanceView, err := vmClient.InstanceView(ctx, rg, *i.Name)
		if err != nil {
			log.Printf("%v", err)
		}

		vmStatus := *instanceView.Statuses

		var vmPowerState string
		for _, v := range vmStatus {
			if strings.Contains(*v.DisplayStatus, "VM") {
				vmPowerState = strings.Trim(*v.DisplayStatus, "VM ")
			} else {
				vmPowerState = "unknown"
			}
		}

		tagsList := make(map[string]string)

		for k, v := range i.Tags {
			tagsList[k] = *v
		}

		ipAddress := getIpAddress(subscriptionId, rg, authorizer, ctx, instanceMetadata)
		configurationState, buildVersion, lastPuppetRun, puppetEvents := parsePuppetData(tagsList, vmID)

		providerData := providerMetadata{
			Name:              *vm.Value().Name,
			VmID:              vmID,
			VmType:            string(instanceMetadata.HardwareProfile.VMSize),
			OperatingSystem:   string(instanceMetadata.StorageProfile.OsDisk.OsType),
			ProvisioningState: *instanceMetadata.VirtualMachineProperties.ProvisioningState,
			PowerState:        vmPowerState,
			IpAddress:         ipAddress,
			ResourceGroup:     rg,
			DiskSize:          getDiskSize(instanceMetadata),
			VmImage:           getImageData(instanceMetadata),
			Tags:              tagsList,
		}

		puppetData := puppetMetadata{
			ConfigurationState: configurationState,
			BuildVersion:       buildVersion,
			LastPuppetRun:      lastPuppetRun,
			PuppetEvents:       puppetEvents,
		}

		vmData{
			ProviderMetadata: providerData,
			PuppetMetadata:   puppetData,
		}.Compose()
	}
}

func authorize() (autorest.Authorizer, context.Context) {
	var metaResult map[string]interface{}

	client := http.Client{
		Timeout: 2 * time.Second,
	}

	azureMetaURI := "http://169.254.169.254/metadata/instance/compute?api-version=2020-06-01"

	metaReq, _ := http.NewRequest("GET", azureMetaURI, nil)
	metaReq.Header.Add("Metadata", "True")
	azResp, _ := client.Do(metaReq)
	json.NewDecoder(azResp.Body).Decode(&metaResult)
	azResp.Body.Close()
	subscriptionId := metaResult["subscriptionId"].(string)

	azCredsFile, _ := ioutil.ReadFile("/tmp/az-creds")
	credsFileIoReader := bytes.NewReader(azCredsFile)
	scanner := bufio.NewScanner(credsFileIoReader)

	line := 1
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "client_id") {
			clientId := strings.TrimSpace(strings.Split(scanner.Text(), ":")[1])
			os.Setenv("AZURE_CLIENT_ID", clientId)
		} else if strings.Contains(scanner.Text(), "client_secret") {
			clientSecret := strings.TrimSpace(strings.Split(scanner.Text(), ":")[1])
			os.Setenv("AZURE_CLIENT_SECRET", clientSecret)
		}
		line++
	}
	os.Setenv("AZURE_SUBSCRIPTION_ID", subscriptionId)

	authorizer, err := auth.NewAuthorizerFromEnvironment()
	if err != nil {
		fmt.Printf("%v", err)
	}
	ctx := context.Background()

	return authorizer, ctx
}

func getPdbHostname(uid string, vmid string) []map[string]interface{} {

	query_string := fmt.Sprintf("[\"and\", [\"=\", [\"fact\", \"deployment_uid\"], \"%s\"], [\"=\", [\"fact\", \"dmi.product.uuid\"], \"%s\"]]", uid, vmid)
	var result []map[string]interface{}

	query_payload := map[string]string{
		"query": query_string,
	}
	json_data, err := json.Marshal(query_payload)
	resp, err := http.Post(pdb_uri+"/facts/fqdn", "application/json", bytes.NewBuffer(json_data))

	if err != nil {
		log.Printf("%v", err)
	}

	json.NewDecoder(resp.Body).Decode(&result)

	return result
}

func getPdbNodeData(uid string, vmid string) []map[string]interface{} {

	query_string := fmt.Sprintf("[\"and\", [\"=\", [\"fact\", \"deployment_uid\"], \"%s\"], [\"=\", [\"fact\", \"dmi.product.uuid\"], \"%s\"]]", uid, vmid)
	var result []map[string]interface{}

	query_payload := map[string]string{
		"query": query_string,
	}
	json_data, err := json.Marshal(query_payload)
	resp, err := http.Post(pdb_uri+"/nodes", "application/json", bytes.NewBuffer(json_data))

	if err != nil {
		log.Printf("%v", err)
	}

	json.NewDecoder(resp.Body).Decode(&result)

	return result
}

func getPdbReport(uid string, vmid string) []map[string]interface{} {

	query_string := fmt.Sprintf("[\"and\", [\"=\", [\"fact\", \"deployment_uid\"], \"%s\"], [\"=\", [\"fact\", \"dmi.product.uuid\"], \"%s\"], [\"=\", \"latest_report?\", \"true\"]]", uid, vmid)
	var result []map[string]interface{}

	query_payload := map[string]string{
		"query": query_string,
	}
	json_data, err := json.Marshal(query_payload)
	resp, err := http.Post(pdb_uri+"/reports", "application/json", bytes.NewBuffer(json_data))

	if err != nil {
		log.Printf("%v", err)
	}

	json.NewDecoder(resp.Body).Decode(&result)

	return result
}

func getPdbEvents(reportHash string) []map[string]interface{} {

	query_string := fmt.Sprintf("[\"=\", \"report\", \"%s\"]", reportHash)
	var result []map[string]interface{}

	query_payload := map[string]string{
		"query": query_string,
	}
	json_data, err := json.Marshal(query_payload)
	resp, err := http.Post(pdb_uri+"/events", "application/json", bytes.NewBuffer(json_data))

	if err != nil {
		log.Printf("%v", err)
	}

	json.NewDecoder(resp.Body).Decode(&result)

	return result
}

func monitor() {
	authorizer, ctx := authorize()
	println("scanning resource groups...")
	resourceGroups := getAzResourceGroups(os.Getenv("AZURE_SUBSCRIPTION_ID"), authorizer, ctx)

	var wg sync.WaitGroup
	println("scanning virtual machines...")
	for _, rg := range resourceGroups {
		wg.Add(1)
		go getAzVms(os.Getenv("AZURE_SUBSCRIPTION_ID"), rg, authorizer, ctx, &wg)
	}
	wg.Wait()

	for _, x := range vmObjectList {
		jsonPayload, _ := json.Marshal(x)
		err := postRedis(x.ProviderMetadata.VmID, jsonPayload)
		if err != nil {
			panic(err)
		}
	}

	println("waiting...")
	time.Sleep(120 * time.Second)
}

func main() {
	for {
		monitor()
	}
}
