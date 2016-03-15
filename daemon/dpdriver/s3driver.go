package dpdriver

import (
	//"fmt"
	"compress/gzip"
	"errors"
	log "github.com/asiainfoLDP/datahub/utils/clog"
	"github.com/asiainfoLDP/datahub/utils/logq"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	s3aws "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"os"
)

var (
	AWS_SECRET_ACCESS_KEY string
	AWS_ACCESS_KEY_ID     string
	AWS_REGION            string
)

type s3driver struct {
}

func (s3 *s3driver) GetDestFileName(dpconn, itemlocation, filename string) (destfilename, tmpdir, tmpfile string) {
	//for s3 dp , use /var/lib/datahub/:BUCKET as the destdir
	destfilename = gDpPath + "/" + dpconn + "/" + itemlocation + "/" + filename
	tmpdir = gDpPath + "/" + dpconn + "/" + itemlocation + "/tmp"
	tmpfile = tmpdir + "/" + filename
	return
}

func (s3 *s3driver) StoreFile(status, filename, dpconn, dp, itemlocation, destfile string) string {
	AWS_SECRET_ACCESS_KEY = Env("AWS_SECRET_ACCESS_KEY", false)
	AWS_ACCESS_KEY_ID = Env("AWS_ACCESS_KEY_ID", false)
	AWS_REGION = Env("AWS_REGION", false)

	file, err := os.Open(filename)
	if err != nil {
		l := log.Error("Failed to open file", err)
		logq.LogPutqueue(l)
		status = "put to s3 err"
		return status
	}

	log.Infof("Begin to upload %v to %v\n", filename, dp)

	// Not required, but you could zip the file before uploading it
	// using io.Pipe read/writer to stream gzip'd file contents.
	reader, writer := io.Pipe()
	go func() {
		gw := gzip.NewWriter(writer)
		io.Copy(writer, file)

		file.Close()
		gw.Close()
		writer.Close()

		//updateJobQueueStatus(jobid, "puttos3ok")
	}()
	uploader := s3manager.NewUploader(session.New(&aws.Config{Region: aws.String(AWS_REGION)}))
	//uploader := s3manager.NewUploader(session.New(aws.NewConfig()))
	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(dpconn),
		Key:    aws.String( /*dp + "/" + */ itemlocation + "/" + destfile + ".gz"),
	})
	if err != nil {
		log.Error("Failed to upload", err)
		status = "put to s3 err"
		return status
	}
	status = "put to s3 ok"
	log.Info("Successfully uploaded to", result.Location)
	return status
}

func (s3 *s3driver) GetFileTobeSend(dpconn, dpname, itemlocation, tagdetail string) (filepathname string) {

	filepathname = gDpPath + "/" + dpconn + "/" + itemlocation + "/" + tagdetail

	if true == isFileExists(filepathname) {
		return
	}

	AWS_SECRET_ACCESS_KEY = Env("AWS_SECRET_ACCESS_KEY", false)
	AWS_ACCESS_KEY_ID = Env("AWS_ACCESS_KEY_ID", false)
	AWS_REGION = Env("AWS_REGION", false)
	file, err := os.Create(filepathname)
	if err != nil {
		log.Error("Failed to create file", err)
		return ""
		defer file.Close()

		downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String(AWS_REGION)}))
		numBytes, err := downloader.Download(file,
			&s3aws.GetObjectInput{
				Bucket: aws.String(dpconn),
				Key:    aws.String( /*dpname + "/" + */ itemlocation + "/" + tagdetail),
			})
		if err != nil {
			log.Info("Failed to download file", err)
			return
		}

		log.Println("Downloaded file", file.Name(), numBytes, "bytes")
		return
	}
	return
}

func (s3 *s3driver) CheckItemLocation(datapoolname, dpconn, itemlocation string) error {
	AWS_SECRET_ACCESS_KEY = Env("AWS_SECRET_ACCESS_KEY", false)
	AWS_ACCESS_KEY_ID = Env("AWS_ACCESS_KEY_ID", false)
	AWS_REGION = Env("AWS_REGION", false)

	svc := s3aws.New(session.New(&aws.Config{Region: aws.String(AWS_REGION)}))
	result, err := svc.ListBuckets(&s3aws.ListBucketsInput{})
	if err != nil {
		log.Println("Failed to list buckets", err)
		return err
	}

	if len(result.Buckets) == 0 {
		return errors.New("Bucket not exist.")
	}
	log.Println("Buckets:")
	for _, bucket := range result.Buckets {
		log.Debugf("%s : %s\n", aws.StringValue(bucket.Name), bucket.CreationDate)
	}

	log.Println(dpconn + "/" + itemlocation)
	err = os.MkdirAll(dpconn+"/"+itemlocation, 0777)
	if err != nil {
		log.Error(err)
	}
	return err
}

func init() {
	//fmt.Println("s3")

	register("s3", &s3driver{})
}

func isFileExists(file string) bool {
	fi, err := os.Stat(file)
	if err == nil {
		log.Println("exist", file)
		return !fi.IsDir()
	}
	return os.IsExist(err)
}
