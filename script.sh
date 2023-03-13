#!/bin/bash

# Define function to wait for specified time
wait_for() {
  echo "Waiting for $1 seconds..."
  sleep $1
}

# Define function to run script and handle errors
run_script() {
  python3 main.py
  exit_code=$?
  
  case $exit_code in
    0)
      # If the script exits with a status of 0, it ran successfully
      break
      ;;
    400|401|403|404|408|429|500|502|503|504)
      echo "HTTP Error, wait for 3 hours before retrying"
      wait_for 10800
      ;;
    111|*Connection\ refused*)
      echo "Connection Error, wait for 2 hours before retrying"
      wait_for 7200
      ;;
    28)
      echo "Timeout Error, wait for 1 hour before retrying"
      wait_for 3600
      ;;
    *)
      echo "Request Exception error, wait for 2 hours before retrying"
      wait_for 7200
      ;;
  esac
}

# Loop to run script and handle errors
while true; do
  run_script
done
