Write-Host "Starting replacement of import statements in the Python scripts"
Python ReplacePythonPath.py
Write-Host "Finished replacement of import statements"
Write-Host "About to delete ReplacePythonPath.py"
Remove-Item ReplacePythonPath.py
Write-Host "ReplacePythonPath.py deleted"