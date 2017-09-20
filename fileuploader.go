package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	promhttp "github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	monitorAddr = kingpin.Flag("monitor.listen-address", "Address on which to expose metrics and pprof web interface.").Default(":9211").String()

	fileUploadAddr = kingpin.Flag("upload.address", "The address to upload file to.").Default("localhost:8080").String()

	scanInterval = kingpin.Flag("scan.interval", "The interval (unit = second) to scan files to upload.").Default("1").Int64()

	//FolderScanInterval is the time interval to scan the metric data file folder
	FolderScanInterval time.Duration

	//FileUploadURL is the URL of File Upload Server.
	FileUploadURL string
)

//coordinate is responsible to scan the metric data files located folder and sends the new filename to todo channel (for file uploading)
func coordinate(wg *sync.WaitGroup, todo chan<- string, done <-chan string, quit <-chan int) {
	wg.Add(1)
	toimp := make(map[string]string, 1000)
	t1 := time.NewTimer(FolderScanInterval)

	for {
		select {
		case <-t1.C:
			t1.Stop()
			//Scan the folder for new file.
			files, err := filepath.Glob("*.gz")
			if err != nil {
				log.Println("Failed to scan *.gz files due to " + err.Error())
				continue
			}

			//Sends new files to todo channel
			for _, f := range files {
				_, ok := toimp[f]
				if !ok {
					toimp[f] = f
					todo <- f
				}
			}

			t1.Reset(FolderScanInterval)
		case f := <-done:
			//Kicks off the uploaded filename from map.
			delete(toimp, f)
		case <-quit:
			t1.Stop()
			log.Println("Coordinator exits!")
			wg.Done()
			return
		}
	}

}

func uploadFile(id int, wg *sync.WaitGroup, todo <-chan string, done chan<- string, quit <-chan int) {

	wg.Add(1)
	var filename string

	for {
		select {
		case filename = <-todo:
			doUpload(filename)
			done <- filename
		case <-quit:
			log.Println("File uploader #" + strconv.Itoa(id) + " exits!")
			wg.Done()
			return
		}
	}

}

func doUpload(filename string) {
	file, err := os.Open(filename)
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	if err != nil {
		log.Println("Failed to open the metric data file named " + filename)
		return
	}

	//Uploads file
	req, err := http.NewRequest("PUT", FileUploadURL+filename, file)
	if err != nil {
		log.Printf("Failed to create a request due to %s \n", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to upload file due to %s \n", err)
		return
	}
	if resp != nil && resp.StatusCode != 200 {
		log.Printf("Failed to upload file due to %s \n", err)
		return
	}

	//Remove the file from disks.
	err = os.Remove(filename)
	if err != nil {
		log.Printf("Failed to delete the uploaded file %s from disk.\n", err)
	}
}

func launchMonitor(monitorAddr *string, wg *sync.WaitGroup) {
	http.Handle("/metrics", promhttp.Handler())
	log.Println(http.ListenAndServe(*monitorAddr, nil))
}

func setReleaseInfo() {
	//	log.AddFlags(kingpin.CommandLine)
	version.BuildUser = "lhe"
	version.BuildDate = "2017.9.20 16:30"
	version.Version = "v1.0.0"
	version.Branch = "master"
	version.Revision = "234a23"
	kingpin.Version(version.Print("fileUploader"))
}

func parseCommandLine() {
	setReleaseInfo()

	kingpin.HelpFlag.Short('h')

	kingpin.Parse()
}

//config loading
//logging
//arguments
//gracefully shutdown
func main() {

	parseCommandLine()

	//Initialization
	FileUploadURL = "http://" + *fileUploadAddr + "/"
	FolderScanInterval = time.Second * time.Duration(*scanInterval)

	var (
		//Channel for filename to import
		todo = make(chan string, 10000)

		//Channel for imported filename
		done = make(chan string, 10000)

		quit = make(chan int)

		wg = new(sync.WaitGroup)
	)

	//Launch goroutines
	rnum := 2
	go coordinate(wg, todo, done, quit)
	go uploadFile(1, wg, todo, done, quit)

	go launchMonitor(monitorAddr, wg)

	//Send quit to goroutines if any signals from OS for gracefully shutdown.
	sc := make(chan os.Signal)
	signal.Notify(sc, syscall.SIGINT, os.Interrupt, os.Kill)
	<-sc

	for i := 0; i < rnum; i++ {
		quit <- i
	}
	wg.Wait()

	//Close the channels
	close(todo)
	close(done)
	close(quit)

	log.Println("Importer exits! See you later!")
}
