package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

var (
	batchSize int    // Batch size for processing rows
	filePath  string // Path to the input file
)

// Struct to hold the min, max, avg stats for each name
type NameStats struct {
	min, max, sum float64
	count          int
}

// Global maps to store stats for each starting letter, and corresponding mutexes for each letter
var nameStatsMap = make(map[rune]map[string]NameStats)
var mapMutexes = make(map[rune]*sync.Mutex)

// Global mutex for protecting access to the nameStatsMap and mapMutexes
var globalMutex sync.Mutex

// Function to process a batch of rows
func processBatch(batch []string, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, line := range batch {
		name, number, err := parseLine(line)
		if err != nil {
			// Handle parsing error, for now just printing it
			fmt.Println("Error parsing line:", err)
			continue
		}
		updateStats(name, number)
	}
}

// Function to parse each line into a name and a number
func parseLine(line string) (string, float64, error) {
	// Split the line by the comma
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid format: %s", line)
	}

	// Extract the name and the number
	name := strings.TrimSpace(parts[0])
	numberStr := strings.TrimSpace(parts[1])

	// Convert the number string to a float64
	number, err := strconv.ParseFloat(numberStr, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid number: %s", numberStr)
	}

	return name, number, nil
}

// Function to safely update the stats for a name
func updateStats(name string, number float64) {
	// Determine the starting letter of the name (case insensitive)
	firstLetter := unicode.ToLower([]rune(name)[0])

	// Lock the global mutex to ensure thread-safe access to nameStatsMap and mapMutexes
	globalMutex.Lock()
	defer globalMutex.Unlock()

	// Lock the mutex for the specific starting letter's map
	mutex, exists := mapMutexes[firstLetter]
	if !exists {
		// If this is the first time we are encountering a letter, initialize the mutex and the map
		mutex = &sync.Mutex{}
		mapMutexes[firstLetter] = mutex

		// Initialize the map for this starting letter
		nameStatsMap[firstLetter] = make(map[string]NameStats)
	}

	// Lock the mutex for the specific starting letter's map
	mutex.Lock()
	defer mutex.Unlock()

	// Get the current stats for the name
	stats, exists := nameStatsMap[firstLetter][name]

	// If the name doesn't exist yet, initialize the stats
	if !exists {
		stats = NameStats{min: number, max: number, sum: number, count: 1}
	} else {
		// Update the min, max, sum, and count based on the new number
		if number < stats.min {
			stats.min = number
		}
		if number > stats.max {
			stats.max = number
		}
		stats.sum += number
		stats.count++
	}

	// Store the updated stats back in the map for this starting letter
	nameStatsMap[firstLetter][name] = stats
}

func main() {
	// Define command-line flags for batch size and file path
	flag.IntVar(&batchSize, "batchSize", 1000, "Number of lines to process in each batch")
	flag.StringVar(&filePath, "file", "yourfile.txt", "Path to the input file")

	// Parse the command-line flags
	flag.Parse()

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Create a buffered reader to read the file line by line
	scanner := bufio.NewScanner(file)

	// Skip the first two lines (comments)
	for i := 0; i < 2; i++ {
		if !scanner.Scan() {
			fmt.Println("Error: file doesn't have enough lines.")
			return
		}
		// Just skip these lines
	}

	var batch []string
	var wg sync.WaitGroup

	// Read the file line by line (after skipping the first two lines)
	for scanner.Scan() {
		line := scanner.Text()
		batch = append(batch, line)

		// Once we have a batch of `batchSize` lines, process it in a new goroutine
		if len(batch) == batchSize {
			wg.Add(1)
			go processBatch(batch, &wg)

			// Clear the batch for the next set of lines
			batch = nil
		}
	}

	// If there are remaining lines in the last batch (less than `batchSize`)
	if len(batch) > 0 {
		wg.Add(1)
		go processBatch(batch, &wg)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	// Print the final result (optional)
	printResults()
}

// Function to print the results
func printResults() {
	// Print out the name -> min/max/avg stats for each starting letter
	for letter, statsMap := range nameStatsMap {
		for name, stats := range statsMap {
			avg := stats.sum / float64(stats.count)
			fmt.Printf("Letter: %c, Name: %s, Min: %.2f, Max: %.2f, Avg: %.2f\n", letter, name, stats.min, stats.max, avg)
		}
	}
}
