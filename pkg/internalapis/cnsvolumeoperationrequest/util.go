package cnsvolumeoperationrequest

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
)

const (
	// CRDName represent the name of cnsvolumeoperationrequest CRD
	CRDName = "cnsvolumeoperationrequests.cns.vmware.com"
	// CRDSingular represent the singular name of cnsvolumeoperationrequest CRD
	CRDSingular = "cnsvolumeoperationrequest"
	// CRDPlural represent the plural name of cnsvolumeoperationrequest CRD
	CRDPlural = "cnsvolumeoperationrequests"
	// MaxEntriesInLatestOperationDetails specifies the maximum length of
	// the LatestOperationDetails allowed in a cnsvolumeoperationrequest instance
	MaxEntriesInLatestOperationDetails = 10
)

// VolumeOperationRequestDetails stores details about a single operation
// on the given volume. These details are persisted by
// VolumeOperationRequestInterface and the persisted details will be
// returned by the interface on request by the caller via this structure.
type VolumeOperationRequestDetails struct {
	name             string
	volumeID         string
	snapshotID       string
	capacity         int64
	operationDetails *operationDetails
}

// OperationDetails stores information about a particular operation.
type operationDetails struct {
	taskInvocationTimestamp metav1.Time
	taskID                  string
	opID                    string
	taskStatus              string
	error                   string
}

func CreateVolumeOperationRequestDetails(name, volumeId, snapshotId string, capacity int64, taskInvocationTimestamp metav1.Time, taskId, opId, taskStatus, error string) *VolumeOperationRequestDetails {
	return &VolumeOperationRequestDetails{
		name:       name,
		volumeID:   volumeId,
		snapshotID: snapshotId,
		capacity:   capacity,
		operationDetails: &operationDetails{
			taskInvocationTimestamp: taskInvocationTimestamp,
			taskID:                  taskId,
			opID:                    opId,
			taskStatus:              taskStatus,
			error:                   error,
		},
	}
}

func convertToCnsVolumeOperationRequestDetails(details operationDetails) *v1alpha1.OperationDetails {
	return &v1alpha1.OperationDetails{
		TaskInvocationTimestamp: details.taskInvocationTimestamp,
		TaskID:                  details.taskID,
		OpID:                    details.opID,
		TaskStatus:              details.taskStatus,
		Error:                   details.error,
	}
}
