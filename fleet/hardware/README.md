# Overview

This repo contains instructions setting up our hardware rig for data collection. This includes

- Avro Schemas for drive meta data
  - The data gets ingested in our S3 storage as defined the schemas
- Hardware setup for drive data collection & data normalisation with our different sensor suites
  - FLIR + OLU : [data_recorder](https://gitlab.mobilityservices.io/am/roam/embedded/data_recorder) from embedded
  - [OnePlus6/T](https://gitlab.mobilityservices.io/am/roam/perception/data-catalogue/wikis/Perception-Datasets#oneplus6t-drives) & [Waylens](https://gitlab.mobilityservices.io/am/roam/perception/data-catalogue/wikis/Perception-Datasets#waylens-drives)

# Submoule
If you plan to use this repo as an added submodule within your project follow these [instructions](https://docs.gitlab.com/ee/ci/git_submodules.html) or [these](https://git-scm.com/book/en/v2/Git-Tools-Submodules)
Instead you can follow the steps below for brievity
```
cd path/to/your/magical-repo
git submodule add ssh://git@ssh.gitlab.mobilityservices.io:443/am/roam/perception/drive-data-configs.git path/to/submodule
cd path/to/submodule
```
Now setup your submodule to point to the `branch` or `tag` need in your magical-repo. Skip the following step if master is all you need. NOTE : submodules don't track branches or tags. They just point to specific commits in the submodule.
```
git checkout <branch_name>
git checkout tags/<tag_you_need>
git status
```
Add the submodule files into you magical-repo.
```
# if needed
git add .gitmodules
# and then
git add path/to/submodule
git commit -am "Added submodule to magical-repo"
git push
```
Now your magical-repo will point to the submodule located at path/to/submodule@<specific_commit> in your gitlab repo. `specific_commit` is set by you `branch_name` or `tag_you_need`

# Gitlab CI

To make GitlabCI work with submodules, two small changes are needed:

- in `.gitmodules` use a relative path for `url` 
- in `.gitlab-ci.yml` add this variable: `GIT_SUBMODULE_STRATEGY: "recursive"`

# Test

This project has a gitlab CI job that will check for full schema compatibility against the latest tag. 

If a new tag is pushed, the test will be executed only if the current tag is in the same major version of the previous one.

To run it locally, checkout the branch, then run: 

       make localTest
