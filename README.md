# try-floci
Try using Floci and play with mocked AWS services

## Accessing the Codespace

Create a Codespace for the repository and add its SSH config to your local SSH setup:

```bash
gh codespace create -r BenSlabbert/try-floci
gh codespace ssh --config >> ~/.ssh/codespaces
echo "Include ~/.ssh/codespaces" >> ~/.ssh/config
ssh codespace-name
```

Replace `codespace-name` with the name returned by `gh codespace list` if you do not already know it.
