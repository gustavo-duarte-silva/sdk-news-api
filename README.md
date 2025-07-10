# sdk-news-api
Fivetran Connector SDK para a api NewsAPI

```
Get-Content test.env | Where-Object { $_ -notmatch '^#' -and $_ -match '=' } | ForEach-Object {
  $name, $value = $_.Split('=', 2)
  $value = $value.Trim('"')
  Set-Content "env:$name" $value
}
```

```
echo $env:FIVETRAN_API_KEY
```