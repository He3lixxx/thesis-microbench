#! /usr/bin/env bash
thread_count=1
memory_size="2g"
timeout=60

set -x

timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -pnative
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -pflatbuf
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -pprotobuf
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -pavro

timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -pcsvstd
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -pcsvfastfloat
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -pcsvbenstrasser

timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -prapidjson
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -prapidjsoninsitu
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -prapidjsonsax

timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -psimdjson
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -psimdjsonec
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -psimdjsonece
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -psimdjsonu
timeout --foreground $timeout ./bench -t$thread_count -m$memory_size -psimdjsonooo
