/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cnsvolumeoperationrequest

import (
	"context"
	"reflect"

	"github.com/davecgh/go-spew/spew"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	csiconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

// VolumeOperationRequest is an interface that supports handling idempotency
// in CSI volume manager. This interface persists operation details invoked
// on CNS and returns the persisted information to callers whenever it is requested.
type VolumeOperationRequest interface {
	// GetRequestDetails returns the details of the operation on the volume
	// that is persisted by the VolumeOperationRequest interface.
	// Returns an error if any error is encountered while attempting to
	// read the previously persisted information.
	// If no previous information has been persisted, it returns an empty object.
	GetRequestDetails(ctx context.Context, name string) (*VolumeOperationRequestDetails, error)
	// StoreRequestDetails persists the details of the operation taking
	// place on the volume.
	// Returns an error if any error is encountered. Clients must assume
	// that the attempt to persist the information failed if an error is returned.
	StoreRequestDetails(ctx context.Context, instance *VolumeOperationRequestDetails) error
}

// operationRequestStoreOnETCD implements the VolumeOperationsRequest interface.
// This implementation persists the operation information on etcd via a client
// to the API server. Reads are also done directly on etcd; there is no caching
// layer involved.
type operationRequestStoreOnETCD struct {
	k8sclient client.Client
}

// InitVolumeOperationRequestInterface creates the CnsVolumeOperationRequest
// definition on the API server and returns an implementation of
// VolumeOperationRequest interface. Clients are unaware of the implementation
// details to read and persist volume operation details.
// This function is not thread safe. Multiple serial calls to this function will
// return multiple new instances of the VolumeOperationRequest interface.
// TODO: Make this thread-safe and a singleton.
func InitVolumeOperationRequestInterface(ctx context.Context) (VolumeOperationRequest, error) {
	log := logger.GetLogger(ctx)

	// Create CnsVolumeOperationRequest definition on API server
	log.Info("Creating cnsvolumeoperationrequest definition on API server")
	err := k8s.CreateCustomResourceDefinitionFromSpec(ctx, CRDName, CRDSingular, CRDPlural,
		reflect.TypeOf(v1alpha1.CnsVolumeOperationRequest{}).Name(), v1alpha1.SchemeGroupVersion.Group, v1alpha1.SchemeGroupVersion.Version, apiextensionsv1beta1.NamespaceScoped)
	if err != nil {
		log.Errorf("failed to create cnsvolumeoperationrequest CRD with error: %v", err)
	}

	// Get in cluster config for client to API server
	config, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		log.Errorf("failed to get kubeconfig with error: %v", err)
		return nil, err
	}

	// Create client to API server
	k8sclient, err := k8s.NewClientForGroup(ctx, config, v1alpha1.SchemeGroupVersion.Group)
	if err != nil {
		log.Errorf("failed to create k8sClient with error: %v", err)
		return nil, err
	}

	// Initialize the operationRequestStoreOnETCD implementation of VolumeOperationRequest
	// interface.
	// NOTE: Currently there is only a single implementation of this interface.
	// Future implementations will need modify this step.
	operationRequestStore := &operationRequestStoreOnETCD{
		k8sclient: k8sclient,
	}

	return operationRequestStore, nil
}

// GetRequestDetails returns the details of the operation on the volume
// that is persisted by the VolumeOperationRequest interface, by querying
// API server for a CnsVolumeOperationRequest instance with the given
// name.
// Returns an error if any error is encountered while attempting to
// read the previously persisted information from the API server.
// If no previous information has been persisted, it returns an empty object.
func (or *operationRequestStoreOnETCD) GetRequestDetails(ctx context.Context, name string) (*VolumeOperationRequestDetails, error) {
	log := logger.GetLogger(ctx)
	instance := &v1alpha1.CnsVolumeOperationRequest{}
	key := client.ObjectKey{
		Name:      name,
		Namespace: csiconfig.DefaultCSINamespace,
	}
	log.Infof("Getting cnsvolumerequestoperation instance with name %s/%s", csiconfig.DefaultCSINamespace, name)
	err := or.k8sclient.Get(ctx, key, instance)
	if err != nil {
		// If instance does not exist, return an empty object
		if errors.IsNotFound(err) {
			log.Infof("CnsVolumeOperationRequest instance not found. Returning ...")
			return nil, nil
		}
		log.Errorf("failed to get cnsvolumeoperation instance %s/%s with error: %v", csiconfig.DefaultCSINamespace, name, err)
		return nil, err
	}
	log.Debugf("Found cnsvolumerequestoperation instance %v", spew.Sdump(instance))

	// Determine which operation details need to be returned. Callers only need to know about the
	// last operation that was invoked on a volume.
	var operationDetailsToReturn v1alpha1.OperationDetails
	if len(instance.Status.LatestOperationDetails) != 0 {
		operationDetailsToReturn = instance.Status.LatestOperationDetails[len(instance.Status.LatestOperationDetails)-1]
	} else {
		operationDetailsToReturn = instance.Status.FirstOperationDetails
	}

	return CreateVolumeOperationRequestDetails(instance.Spec.Name, instance.Status.VolumeID, instance.Status.SnapshotID,
			instance.Status.Capacity, operationDetailsToReturn.TaskInvocationTimestamp, operationDetailsToReturn.TaskID,
			operationDetailsToReturn.OpID, operationDetailsToReturn.TaskStatus, operationDetailsToReturn.Error),
		nil
}

// StoreRequestDetails persists the details of the operation taking
// place on the volume by storing it on the API server.
// Returns an error if any error is encountered. Clients must assume
// that the attempt to persist the information failed if an error is returned.
func (or *operationRequestStoreOnETCD) StoreRequestDetails(ctx context.Context, operationToStore *VolumeOperationRequestDetails) error {
	log := logger.GetLogger(ctx)
	log.Debugf("Storing nsvolumerequestoperation instance with spec %v", spew.Sdump(operationToStore))

	operationDetailsToStore := convertToCnsVolumeOperationRequestDetails(*operationToStore.operationDetails)

	instance := &v1alpha1.CnsVolumeOperationRequest{}
	log.Infof("Getting cnsvolumerequestoperation instance with name %s/%s", csiconfig.DefaultCSINamespace, operationToStore.name)

	namespacedName := client.ObjectKey{Name: operationToStore.name, Namespace: csiconfig.DefaultCSINamespace}
	if err := or.k8sclient.Get(ctx, namespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			// Create new instance on API server if it doesnt exist. Implies that this is the first time this object is being stored.
			newInstance := &v1alpha1.CnsVolumeOperationRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:      namespacedName.Name,
					Namespace: namespacedName.Namespace,
				},
				Spec: v1alpha1.CnsVolumeOperationRequestSpec{
					Name: namespacedName.Name,
				},
				Status: v1alpha1.CnsVolumeOperationRequestStatus{
					VolumeID:               operationToStore.volumeID,
					SnapshotID:             operationToStore.snapshotID,
					Capacity:               operationToStore.capacity,
					FirstOperationDetails:  *operationDetailsToStore,
					LatestOperationDetails: nil,
				},
			}
			err = or.k8sclient.Create(ctx, newInstance)
			if err != nil {
				log.Errorf("failed to create cnsvolumeoperationrequest instance %s/%s with error: %v", namespacedName.Namespace, namespacedName.Name, err)
				return err
			}
			log.Infof("Created cnsvolumeoperationrequest instance %s/%s with latest information for task with ID: %s", namespacedName.Namespace, namespacedName.Name, operationDetailsToStore.TaskID)
			return nil
		}
		log.Errorf("failed to get cnsvolumeoperationrequest instance %s/%s with error: %v", namespacedName.Namespace, namespacedName.Name, err)
		return err
	}

	// Create a deep copy since we modify the object
	updatedInstance := instance.DeepCopy()

	// Modify VolumeID, SnapshotID or Capacity
	updatedInstance.Status.VolumeID = operationToStore.volumeID
	updatedInstance.Status.SnapshotID = operationToStore.snapshotID
	updatedInstance.Status.Capacity = operationToStore.capacity

	// Modify Status.FirstOperationDetails only if it doesnt exist or TaskID's match
	firstOp := instance.Status.FirstOperationDetails
	if firstOp.TaskID == "" || firstOp.TaskID == operationToStore.operationDetails.taskID {
		updatedInstance.Status.FirstOperationDetails = *operationDetailsToStore
		err := or.k8sclient.Update(ctx, updatedInstance)
		if err != nil {
			log.Errorf("failed to update cnsvolumeoperationrequest instance %s/%s with error: %v", namespacedName.Namespace, namespacedName.Name, err)
			return err
		}
		log.Infof("Updated cnsvolumeoperationrequest instance %s/%s with latest information for task with ID: %s", namespacedName.Namespace, namespacedName.Name, operationDetailsToStore.TaskID)
		return nil

	}

	// If the Task details already exist in the Status, update it with the latest information
	for index, operationDetails := range instance.Status.LatestOperationDetails {
		if operationDetailsToStore.TaskID == operationDetails.TaskID {
			updatedInstance.Status.LatestOperationDetails[index] = *operationDetailsToStore
			err := or.k8sclient.Update(ctx, updatedInstance)
			if err != nil {
				log.Errorf("failed to update cnsvolumeoperationrequest instance %s/%s with error: %v", namespacedName.Namespace, namespacedName.Name, err)
				return err
			}
			log.Infof("Updated cnsvolumeoperationrequest instance %s/%s with latest information for task with ID: %s", namespacedName.Namespace, namespacedName.Name, operationDetailsToStore.TaskID)
			return nil
		}
	}

	// Append the latest Task details to the instance and ensure length of LatestOperationDetails is not greater than 10
	updatedInstance.Status.LatestOperationDetails = append(updatedInstance.Status.LatestOperationDetails, *operationDetailsToStore)
	if len(updatedInstance.Status.LatestOperationDetails) > MaxEntriesInLatestOperationDetails {
		updatedInstance.Status.LatestOperationDetails = updatedInstance.Status.LatestOperationDetails[1:]
	}
	err := or.k8sclient.Update(ctx, updatedInstance)
	if err != nil {
		log.Errorf("failed to update cnsvolumeoperationrequest instance %s/%s with error: %v", namespacedName.Namespace, namespacedName.Name, err)
		return err
	}
	log.Infof("Updated cnsvolumeoperationrequest instance %s/%s with latest information for task with ID: %s", namespacedName.Namespace, namespacedName.Name, operationDetailsToStore.TaskID)
	return nil
}
