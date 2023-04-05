#!/bin/bash
python3 initialize.py
uvicorn app:app --host '0.0.0.0' --port 3000 --reload