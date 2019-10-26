package main

import (
	"github.com/coocood/badger/minio"
)
func main() {
	minioClient := minio.InitMinioClient()
	minioClient.GetObject("sicp3.jpg", "/Users/xern/code/new2.jpg")
	minioClient.PutObject("sicp4.jpg", "/Users/xern/code/new2.jpg")
	minioClient.PutObject("sicp5.jpg", "/Users/xern/code/new2.jpg")
	minioClient.RMObject("sicp5.jpg")
}
