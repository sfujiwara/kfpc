# Contribution Guideline

## Release

1. Create release branch
   - Increment version numbers
   - Build and push Docker image
2. Merge release branch
3. Create release tag and release note via release-it

### Increment version numbers

- `setup.py`
- `pyproject.toml`
- `docker-compose.yaml`

### Build and push Docker image

```shell
docker-compose build
docker-compose push
```

### Generate git tag and release note via release-it

```shell
npx release-it
```
