# Workflow

## Commits

### Make small and focused commits
- Please avoid mixing unrelated changes in a single commit
- Commit at regular points to revert changes easily if needed

### Write clear commit messages
- Limit subject line to 50 characters
- Provide more detailed explainations in the commit body (if required)
- Use the imperative mood in the subject line (e.g. 'add' instead of 'added')

```
$ ~ git commit
```

```
[Type]: [Short Subject]
---[Blank Line]---
[Body, Limit to 72 Characters]
```
- Types: feat, fix, docs, style, refactor, test, chore, ci, perf
  - See [here](https://eagerworks.com/blog/conventional-commits) for more information

## Branches

### Naming Conventions

- Use lowercase with hyphens
- Include type and change with small description

```
[type]/[brief-description] :: e.g. feature/api
```
### Base Branch

- Branch from `develop` for features and non-urgent fixes
- Branch off from `main` for urgent changes (project deadline) - this should be rarely used

### Keep branches updated

- Regularly merge and also delete branches when stale

## PRs

1. Create a pull request for each feature or fix (link to related issues)
2. Write a clear description which...
   1. Summarises the changes
   2. Explains the reasoning behind the changes
   3. Lists any areas of concerns (i.e. breaking changes)
3. Keep PRs focused - split changes into multiple PRs if needed
4. Assign someone to review
5. Merge ONLY after team approval - resolve conflicts & ensure CI checks pass
6. Use [squash and merge](https://learn.microsoft.com/en-us/azure/devops/repos/git/merging-with-squash?view=azure-devops) when needed to keep main branch history clean