package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

func main() {
	examplesDir := "./examples"

	entries, err := os.ReadDir(examplesDir)
	if err != nil {
		fmt.Printf("Error reading examples directory: %v\n", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	errChan := make(chan string, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			wg.Add(1)
			go runExample(filepath.Join(examplesDir, entry.Name()), entry.Name(), &wg, errChan)
		}
	}

	wg.Wait()
	close(errChan)

	errors := []string{}
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		fmt.Println("The following examples failed:")
		for _, err := range errors {
			fmt.Println(err)
		}
		os.Exit(1)
	}

	fmt.Println("All examples ran successfully!")
}

func runExample(dir, name string, wg *sync.WaitGroup, errChan chan<- string) {
	defer wg.Done()

	cmd := exec.Command("go", "run", ".")
	cmd.Dir = dir

	output, err := cmd.CombinedOutput()
	if err != nil {
		errChan <- fmt.Sprintf("Example %s failed: %v\nOutput:\n%s", name, err, output)
	}
}