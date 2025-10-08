while ($true) {
    try {
        $source = "C:\Users\mad\git\met-downloader\images-commons"
        $destination = "\\TINYTIM\Quaffle\Multimedia\Misc\museum pieces and paintings"

        Write-Host "Moving files from $source to $destination..."

        mv "$source\*" "$destination" -Include *.png,*.jpg -Force
        Write-Host "Move completed at $(Get-Date). Waiting 60 seconds..."
    }
    catch {
        Write-Host "Error: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds 60
}
