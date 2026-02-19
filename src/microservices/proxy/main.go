package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	monolithURL      = mustParse(os.Getenv("MONOLITH_URL"))
	moviesServiceURL = mustParse(os.Getenv("MOVIES_SERVICE_URL"))
	eventsServiceURL = mustParse(os.Getenv("EVENTS_SERVICE_URL"))

	gradualMigration = os.Getenv("GRADUAL_MIGRATION") == "true"
	migrationPercent = getPercent(os.Getenv("MOVIES_MIGRATION_PERCENT"))
)

func main() {
	rand.Seed(time.Now().UnixNano())

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	log.Println("CinemaAbyss Proxy started on port:", port)
	log.Println("Gradual migration:", gradualMigration)
	log.Println("Movies migration percent:", migrationPercent)

	http.HandleFunc("/", handleRequest)

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleRequest(w http.ResponseWriter, r *http.Request) {

	target := route(r)

	log.Printf("Routing %s %s → %s\n", r.Method, r.URL.Path, target.Host)

	proxy := httputil.NewSingleHostReverseProxy(target)

	// чтобы корректно работал Host header
	r.Host = target.Host

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Println("Proxy error:", err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
	}

	proxy.ServeHTTP(w, r)
}

func route(r *http.Request) *url.URL {

	path := r.URL.Path

	// health gateway
	if path == "/health" {
		return monolithURL
	}

	// movies domain
	if strings.HasPrefix(path, "/api/movies") {
		return routeMovies()
	}

	// events domain
	if strings.HasPrefix(path, "/api/events") {
		return eventsServiceURL
	}

	// default → monolith
	return monolithURL
}

func routeMovies() *url.URL {

	// если постепенность отключена → 100% в movies
	if !gradualMigration {
		return moviesServiceURL
	}

	// крайние случаи
	if migrationPercent <= 0 {
		return monolithURL
	}
	if migrationPercent >= 100 {
		return moviesServiceURL
	}

	// случайное распределение
	random := rand.Intn(100)

	if random < migrationPercent {
		return moviesServiceURL
	}

	return monolithURL
}

func mustParse(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatal(err)
	}
	return u
}

func getPercent(value string) int {
	p, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	if p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}
