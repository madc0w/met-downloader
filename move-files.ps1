while ($true) {
    try {
        $source = "C:\Users\mad\git\met-downloader\images-commons"
        $destination = "\\TINYTIM\Quaffle\Multimedia\Misc\science and tech"

        Write-Host "Moving files from $source to $destination..."

        mv -Force "$source\*.png" "$destination"
        mv -Force "$source\*.jpg" "$destination"
        mv -Force "$source\*.jpeg" "$destination"
        mv -Force "$source\*.webp" "$destination"
        Write-Host "Move completed at $(Get-Date). Waiting 60 seconds..."
    }
    catch {
        Write-Host "Error: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds 60
}
