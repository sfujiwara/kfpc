# Contribution Guideline

## Release

1. Create release branch `release/${VERSION}`
   - Increment version numbers in [`pyproject.toml`](pyproject.toml)
   - Build and push Docker image
2. Merge release branch to `main` branch
3. Create release tag and release note via release-it

### Build and push Docker image

```shell
inv docker.build
inv docker.push
```

### Generate git tag and release note via release-it

```shell
npx release-it
```
