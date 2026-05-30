# Contributing

## Legal

By submitting a pull request, you represent that you have the right to license your contribution to the community, and agree by submitting the patch that your contributions are licensed under the Apache 2.0 license (see [LICENSE](LICENSE)).

## Contributor Conduct

Contributors are expected to follow the project's [Code of Conduct](CODE_OF_CONDUCT.md).

## Submitting a bug or issue

Please ensure to include the following in your bug report:

- A concise description of the issue, what happened, and what you expected.
- Simple reproduction steps.
- The commit, branch, or release of the library you are using.
- Contextual information, including Swift version, OS, and SQL Server version.

## Submitting a Pull Request

Please ensure to include the following in your Pull Request:

- A description of what you are trying to do.
- A description of the code changes.
- Documentation on how these changes are being tested.
- Additional tests to show your code working and to ensure future changes do not break your code.

Please keep your PRs to a minimal number of changes. If a PR is large, try to split it up into smaller PRs. Don't move code around unnecessarily; it makes comparing old with new very hard.

The main development branch of the repository is `main`.

### Formatting

We use `swift-format` for formatting code.
Formatting and additional checks can be run using `./scripts/soundness.sh`.
Please run these checks before opening a PR.
