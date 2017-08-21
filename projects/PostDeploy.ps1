Write-Host "Starting replacement of import statements in the Python scripts"

$python = "C:\ProgramData\Anaconda2\python.exe"

$projectName = $OctopusParameters["ProjectName"]

Write-Host "Project name is: " + $projectName

$pythonScript = "d:\Executables\PythonScripts\" + $projectName + "\ReplacePythonPath.py"

Write-Host "Script path is: " + $pythonScript

$python $pythonScript

Write-Host "Finished replacement of import statements"
Write-Host "About to delete ReplacePythonPath.py"
Remove-Item ReplacePythonPath.py
Write-Host "ReplacePythonPath.py deleted"