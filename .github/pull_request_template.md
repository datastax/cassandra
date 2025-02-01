### What is the issue
...

### What does this PR fix and why was it fixed
...

### Checklist before you submit for review
- [ ] Make sure there is a PR in the CNDB project updating the Converged Cassandra version if your PR is changing the API
- [ ] Use `NoSpamLogger` for log lines that may appear frequently in the logs
- [ ] Verify Butler and Sonar checks are passing
- [ ] Proper code formatting
- [ ] Proper title for each commit staring with the project-issue number, like CNDB-1234
- [ ] Each commit has a meaningful description
- [ ] Each commit contains related changes
- [ ] Renames, moves and reformatting are in distinct commits
- [ ] Avoid starting nodes in new tests (jvm dtests, CQLTester) if possible in favor of implementing pure unit tests
