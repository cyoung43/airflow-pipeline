# Create hooks folder if it doesn't exist
try {
    New-Item -Path '.githooks' -ItemType Directory -ea Stop
}
catch {
    if ($_.Exception.Message -like '*already exists*') {
        Write-Warning ".githooks folder already exists"
    } else {
        throw 
    }
}

try {
    $FolderName = ".git\hooks"
    if (Test-Path $FolderName) {
        # Perform Delete file from folder operation
        Remove-Item '.git\hooks' -Recurse -ea Stop
    }
    else
    {
        #PowerShell Create directory if not exists
        New-Item $FolderName -ItemType Directory
    }
    # Create Folder Junction
    New-Item -ItemType Junction -Path .git/hooks -Target .githooks
}
catch {
    if ($_.Exception.Message -like '*NTFS junction*') {
        Write-Warning "Setup not needed, junction exists"
    } else {
        throw 
    }
}

# Install dependancy required for created dbt yaml schema file.
pip install pyyaml