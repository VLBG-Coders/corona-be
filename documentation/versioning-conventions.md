# Versioning Conventions

## Feature/Bugfix/Enhancement Branches

*Branch name:* `[<jira-ticket-number>]`

*Example:* `ac-666`

These branches are made for each change as usual. For bugfixs and hotfixes keep in mind to double check the origin of your branch.

## Tags / Versions

*Tag name:* `<Major Version>.<Minor Version>.<Bugfix Version>`

*Example:* `1.5.1`

Tags are used for releases. Tags should never be updated or deleted. Add a new tag for each bugfix or feature.

## Changelog

Add each version update as a new headline in the [CHANGELOG.md](../CHANGELOG.md).

**Newly added changes**

For new features, bugfixes, hotfixes or enhancements add a new changelog entry below the `unreleased` header.

*Example:*

```
# Project Name

#### unreleased

- [FEATURE]: AW-102: Added a new api service for racing cars.

#### 2018-12-12

- [FEATURE]: AW-101: Added a new page for famous people.
- [BUGFIX]: AW-100: Fixed typo in documentation.
```

**Production release**

If a production release is done add a new date headline above the latest change of the `unreleased` block. The date represents the date of the production release.

*Example:*

```
# Project Name

#### unreleased

#### 2018-12-13

- [FEATURE]: AW-102: Added a new api service for racing cars.

#### 2018-12-12

- [FEATURE]: AW-101: Added a new page for famous people.
- [BUGFIX]: AW-100: Fixed typo in documentation.
```
