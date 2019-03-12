package blob

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
)

func init() {
	activity.Register(&Activity{}, New)
}

var activityMd = activity.ToMetadata(&Settings{}, &Input{}, &Output{})

type Activity struct {
	settings *Settings
	logger   log.Logger
}

func New(ctx activity.InitContext) (activity.Activity, error) {
	s := &Settings{}
	err := metadata.MapToStruct(ctx.Settings(), s, true)
	if err != nil {
		return nil, err
	}

	act := &Activity{settings: s, logger: ctx.Logger()}
	return act, nil
}

func (a *Activity) Metadata() *activity.Metadata {
	return activityMd
}

func handleErrors(err error, log log.Logger) error {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:

				return errors.New("Received 409. Container already exists")
			}
		}
		log.Info(err)
	}

	return nil
}

func (a *Activity) Eval(ctx activity.Context) (done bool, err error) {
	input := &Input{}
	ctx.GetInputObject(input)
	accountName, accountKey := a.settings.AZURE_STORAGE_ACCOUNT, a.settings.AZURE_STORAGE_ACCESS_KEY

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		a.logger.Debugf("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	containerName := a.settings.ContainerName

	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	containerURL := azblob.NewContainerURL(*URL, p)
	bctx := context.Background()

	a.logger.Infof("Executing method ", a.settings.Method)
	switch a.settings.Method {

	case "upload":
		a.logger.Infof("Creating a container named %s\n", containerName)

		// This example uses a never-expiring context
		_, err = containerURL.Create(bctx, azblob.Metadata{}, azblob.PublicAccessNone)
		if err != nil {
			return true, err
		}
		err = handleErrors(err, a.logger)

		if err != nil {
			return true, err
		}
		a.logger.Info("Creating a dummy file to test the upload and download\n")
		err = ioutil.WriteFile(input.File, []byte(input.Data), 0700)
		err = handleErrors(err, a.logger)

		if err != nil {
			return true, err
		}

		// Here's how to upload a blob.
		blobURL := containerURL.NewBlockBlobURL(input.File)
		file, err := os.Open(input.File)
		err = handleErrors(err, a.logger)

		if err != nil {
			return true, err
		}

		a.logger.Infof("Uploading the file with blob name: %s\n", input.File)
		_, err = azblob.UploadFileToBlockBlob(bctx, file, blobURL, azblob.UploadToBlockBlobOptions{
			BlockSize:   4 * 1024 * 1024,
			Parallelism: 16})

	case "list":
		out := &Output{}
		a.logger.Info("Listing the blobs in the container:")
		for marker := (azblob.Marker{}); marker.NotDone(); {
			// Get a result segment starting with the blob indicated by the current Marker.
			listBlob, err := containerURL.ListBlobsFlatSegment(bctx, marker, azblob.ListBlobsSegmentOptions{})
			err = handleErrors(err, a.logger)

			if err != nil {
				return true, err
			}

			// ListBlobs returns the start of the next segment; you MUST use this to get
			// the next segment (after processing the current result segment).
			marker = listBlob.NextMarker

			// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
			for _, blobInfo := range listBlob.Segment.BlobItems {
				a.logger.Infof(" Blob name: " + blobInfo.Name + "\n")
				out.Result[blobInfo.Name] = blobInfo

			}
		}
		ctx.SetOutputObject(out)

	}
	return true, nil
}
