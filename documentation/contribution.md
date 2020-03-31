# Contribution

## 1. Branches

In order to make it easier to reproduce the change history on github the following rules for branch names have to be respeced:

- Branch names have to be named by the jira/trello ticket number to automatically track all activities in jira/trello. e.g. `ac-666`
- Branches have to be deleted after they were merged.

## 2. Changelog

A new log message in the [CHANGELOG.md](../CHANGELOG.md) has to be written for each change in this project. Also add the issue or pull request number to each line.
New features or bugfixes have to be written below the `unreleased` header.

**Changelog entry syntax:**

```
#### unreleased

- [FEATURE|ENHANCEMENT|BUGFIX|HOTFIX] {TeamShortName}-{TicketNumber}: {Change log entry}
```

**Changelog entry example:**

```
#### unreleased

- [FEATURE] AW-102: Added user service and permission handling.
```

## 3. Documentation

### Features / Enhancements

Each new feature or enhancement has to be described in short within the [features documentation](features.md). This helps to maintain an overview about the functions provided by the project and helps the developer to quickly decide if a behaviour is excpeted or considered to be a bug.

### APIs

If apis are created the following has to be documented:

- Endpoint (url)
- Possible request methods
- Possible request parameters or post body
- Expected response type and structure
- Expected error behavour

Find an api documentation example [here](apis.md).

## 4. Pull requests

Each change on the project has to be reviewed and approved by an other developer. Therefor a new pull request for each change has to be made. Please respect the following rules in order to make it easier to review your changes:

- Pull requests are only allowed the merged when they were approved.
- Pull requests need a title to describe what the pull request is about. If a complex problem is solved a short description is very helpful.
- Create small and clear pull requests.
  - If you are creating a large feature split your changes up and merge them into some parent feature branch.
  - Large pull requests will lead to inefficient reviews and cost a lot of time.
- Always check your pull request to common misstakes or typos before requesting a review by someone else.
- Request a review by posting the url of your pull request to the slack channel and mention the person you want to review it.

## 5. Level of done

**Contribution Checklist:**

- New branch has been created.
- Changes are committed and pushed.
- Test are running successfully.
- Jenkins build is successful.
- If new functions need to be tested, tests are implemented.
- Documentation was extended / updated.
- Feature branch has been tested on the test environment.
- Pull request was created and reviewed by an other developer.
- Pull request has been checked to be fast forward before merging.
- If there are merge conflicts, the source branch is merged INTO the feature branch, and merge conflicts are solved.
- Pull request has been merged into the source branch by the creator of the pull request.
- Merged branch is deleted on github.
- The [feature|bugfix|enhancement] has been shown and accepted by the product owner.

## 6. Deployment

Find all deployment information in the [deployment documentation](deployment.md).
