package main

import (
	"github.com/coocood/badger/minio"
	"log"
)

func main() {
	minioClient := minio.InitMinioClient()
	for i:=0; i<100 ; i++ {
		err := minioClient.GetObject("sicp3.jpg", "/Users/xern/code/new2.jpg")
		if err != nil {
			log.Println(err)
		}
		err = minioClient.PutObject("sicp4.jpg", "/Users/xern/code/new2.jpg")
		if err != nil {
			log.Println(err)
		}
		err = minioClient.PutObject("sicp5.jpg", "/Users/xern/code/new2.jpg")
		if err != nil {
			log.Println(err)
		}
		err = minioClient.RMObject("sicp5.jpg")
		if err != nil {
			log.Println(err)
		}
	}

}
