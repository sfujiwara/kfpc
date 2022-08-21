# Contribution Guideline

## Release

Below is the branching model:

```mermaid
gitGraph
  commit
  branch feature1
  commit
  commit
  checkout main
  branch feature2
  commit
  commit
  checkout main
  merge feature1
  branch release/0.1.0
  commit
  checkout main
  merge release/0.1.0
  commit tag: "v0.1.0" type: HIGHLIGHT
  merge feature2
  branch release/0.1.1
  commit
  checkout main
  merge release/0.1.1
  commit tag: "v0.1.1" type: HIGHLIGHT
```

### Release Branch

1. Create release branch `release/${VERSION}`
2. Increment version numbers in [`pyproject.toml`](pyproject.toml)
3. Build and push Docker image
   ```shell
   inv docker.push
   ```
4. Create pull request and merge it to `main` branch

### Git Tag and Release Note

- Create release tag
- [Generate GitHub release note automatically](https://docs.github.com/en/repositories/releasing-projects-on-github/automatically-generated-release-notes)
