#!/bin/sh

# Check if the branch name is provided as an argument
if [ -z "$1" ]; then
  BRANCH=""
else
  BRANCH="@$1"
fi

yes Y | pip uninstall prefect_airbyte
echo "Installing prefect_airbyte from branch $BRANCH. Defaults to main if no branch is provided."
yes Y | pip install prefect_airbyte@git+https://github.com/Ishankoradia/prefect-airbyte$BRANCH