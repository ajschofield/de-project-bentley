# Workflow

## References

https://nvie.com/posts/a-successful-git-branching-model/ \
https://learn.microsoft.com/en-us/azure/devops/repos/git/merging-with-squash?view=azure-devops


## Branching

*Based off GitFlow but slightly modified*

- There are two main branches
  - `main` - production-ready code
  - `development` - integration branch for features
  - `staging` - represents the current staging state
- In addition, there are additional branches 
  - Feature branches - for new features and non-urgent bugfixes
  - Hotfix branches - probably won't be used but for critical bugs in production (this is what testing should prevent)
  - Release branches - for preparation of production releases

- Feature branches - e.g. `feature/short-description`
- Bugfix branches - e.g. `bugfix/short-description`
- Hotfix branches - e.g. `hotfix/short-description`
- Release branches - e.g. `release/vX.Y.Z`

### Examples
```
feature/add-data-extractor
bugfix/fix-s3-upload-error
hotfix/security-patch
release/v1.0.0
```

## Environments

1. Development - where active development and initial testing occur
2. Staging - for integration testing and final checks before production
3. Production - live and stable environment

## Deployment

1. `main` - represents the current production state
2. `develop` - represents the integration branch for features and non-urgent fixes
3. `staging` - represents the current staging state

## Staging Flow

1. Create feature branches from `develop` & merge completed features back into `develop`
2. When the `develop` branch is ready for testing, create a `staging` branch from `develop`
3. Deploy the `staging` branch to the staging environment and perform our unit-tests
4. If staging tests pass, create a `release/vX.Y.Z` branch from `staging`
5. Make any final adjustments in the `release/vX.Y.Z` branch
6. Once we have approved the changes in the `release/vX.Y.Z` branch, merge into `main`
7. Tag the release in `main`

### Notes

- No new features should be included in the release branches and any new features should be merged into `develop` for the next release cycle

## Commit Messages

Please follow the conventional commits specification:
  
```
<type>[optional scope]: <description>

<optional body>

[optonal footer(s)]
```

### Types
- feat: new features
- fix: bugfixes
- docs: documentation-only changes
- style: changes that do not affect the meaning of the code
- refactor: code changes that neither fix bugs nor adds features
- perf: code changes that improve performance
- test: adding tests or correcting existing tests
- chore: changes to build process or tools/libraries (probably not needed)
- infra: changes to infrastructure configuration (e.g. Terraform)

### Examples
```
feat(extract): add automatic scheduling for data ingestion
docs: update README with project setup instructions
```

Configuration files for things such as Terraform isn't native to Conventional Commits, but we can add our own:

```
infra(tf): update S3 bucket policy
```

If the Terraform change involves a fix, you may combine `fix` and `infra`:

```
fix(infra): ...
```
