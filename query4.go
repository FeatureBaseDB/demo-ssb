package main

import (
	"fmt"
	"net/http"
	"time"
)

/*
4.1  group by (year, c_nation)            order by (year, c_nation)
4.2  group by (year, s_nation, category), order by (year, s_nation, category)
4.3  group by (year, s_city, brand),      order by (year, s_city, brand)
*/

func (s *Server) HandleQuery41(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 4.1: %f sec\n", seconds)
}
func (s *Server) HandleQuery42(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 4.2: %f sec\n", seconds)
}
func (s *Server) HandleQuery43(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	seconds := time.Now().Sub(start).Seconds()
	fmt.Printf("query 4.3: %f sec\n", seconds)
}
