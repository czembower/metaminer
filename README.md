# metaminer

1. retrieves information about a virtual machine running in azure from the azure metadata service
1. extracts related information about the virtual machine from a puppetdb service running inside k8s
1. resulting metadata is parsed and merged with any existing records in an adjacent redis cluster
