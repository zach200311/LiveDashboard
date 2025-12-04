#!/usr/bin/env bash
cd "$(dirname "$0")/charts"
python -m http.server 8080 -d .


@echo off
cd /d %~dp0\charts
python -m http.server 8080 -d .
