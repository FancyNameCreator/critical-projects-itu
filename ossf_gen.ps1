$input_csv_path = $args[0]
$output_csv_path = $args[1]
$ossf_tool_path = $args[2]

$packages = Import-Csv -Path $input_csv_path

foreach ($package in $packages)
{
  Write-Host "===================================================================================================="
  Write-Host "Processing " $package.name
  
  $cmdOutput = python $ossf_tool_path --repo $package.repository --format json --use_only dependents_count contributor_count created_since open_issues_count --pkg_and_mng $package.name $package.pkg_mng

  if (-Not ([string]::IsNullOrEmpty($cmdOutput)))
  {
    $cmdObject = $cmdOutput | ConvertFrom-Json

    if ($cmdObject.calculation_type.Contains("PageRank"))
    {
        $objResults = New-Object PSObject -Property @{
            Name                = $package.name;
            PackageManager      = $package.pkg_mng;
            Repository          = $package.repository;
            CriticalityScore    = $cmdObject.criticality_score;
            PageRank            = $cmdObject.package_pagerank;
            CreatedSince        = $cmdObject.created_since;
            OpenIssuesCount     = $cmdObject.open_issues_count;
            ContributorCount    = $cmdObject.contributor_count;
        }

        $objResults | Export-CSV $output_csv_path -Append -Force -NoTypeInformation
    }
  }
}