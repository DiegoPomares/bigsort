package main

import (
    "sort"
    "unicode"
    "unicode/utf8"
)

// Return true if string i goes before j
func compare(str_i, str_j string) bool {
    //return str_i < str_j  // lexicographical comparison, faster but crappier

    for len(str_i) > 0 && len(str_j) > 0 {
        ri, size_i := utf8.DecodeRuneInString(str_i)
        rj, size_j := utf8.DecodeRuneInString(str_j)

        ri_lower := unicode.ToLower(ri)
        rj_lower := unicode.ToLower(rj)

        if ri_lower != rj_lower {
            return ri_lower < rj_lower
        }

        str_i = str_i[size_i:]
        str_j = str_j[size_j:]
    }

    return false
}

type Alphabetical []string

// Sort interface
func (self Alphabetical) Len() int {  // Return number of items
    return len(self)
}
func (self Alphabetical) Swap(i, j int) {  // Swap indexes between items
    self[i], self[j] = self[j], self[i]
}
func (self Alphabetical) Less(i, j int) bool {  // Comparision function
    return compare(self[i], self[j])
}


type Line struct {
    value    string
    idx      int
}
type LineHeap []Line

// Sort interface
func (self LineHeap) Len() int {  // Return number of items
    return len(self)
}
func (self LineHeap) Swap(i, j int) {  // Swap indexes between items
    self[i], self[j] = self[j], self[i]
}
func (self LineHeap) Less(i, j int) bool {  // Comparision function
    return compare(self[i].value, self[j].value)
}

// Heap interface
func (self *LineHeap) Push(x interface{}) {  // Append item
    *self = append(*self, x.(Line))
}
func (self *LineHeap) Pop() interface{} {  // Get last item
    h_len := len(*self)
    value := (*self)[h_len-1]
    *self = (*self)[:h_len-1]
    return value
}


func Heapsort(data sort.Interface) {
    data_len := data.Len()

    // Heapify
    for idx := (data_len - 1)/2; idx>=0; idx-- {
        siftDown(data, idx, data_len)
    }

    // Sorting
    for idx := data_len - 1; idx >= 0; idx-- {
        data.Swap(0, idx)
        siftDown(data, 0, idx)
    }
}

func siftDown(data sort.Interface, start, end int) {
    root := start

    for {

        child := 2*root + 1

        // Out of index, nothing to do
        if child >= end {
            return
        }

		// Select the greatest child
        if child + 1 < end && data.Less(child, child + 1) {
            child++
        }

		// Child is smaller than root, nothing to do
        if !data.Less(root, child) {
            return
        }

		// Child is greater than the root, swap them
        data.Swap(root, child)
        root = child
    }
}
