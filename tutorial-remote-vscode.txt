How to Remote VSCode to Google Cloud VM Instances

1. Open https://cloud.google.com/compute/docs/connect/create-ssh-keys
2. Scroll to Create an SSH key pair and choose your OS
3. Edit "Windows User", "File Name", and "Username"
4. Copy the code, paste to your terminal, and 
5. The SSH key will be generated to your folder
6. Open your SSH public key and copy all the text
7. Open https://console.cloud.google.com/compute/metadata?project=data-fellowship-batch-7&tab=sshkeys
8. Click Edit >> +ADD ITEM
9. Paste your text to SSH key field >> SAVE
10. Open terminal and write (Find the external IP address at https://console.cloud.google.com/compute/instances?project=data-fellowship-batch-7)
```
ssh -i ~/.ssh/<file-name> <username>@<external-ip>
```
For the next step is optional. It used to make you connect to VM more easy
11. Create a config file inside your folder
```
touch config (FOR LINUX ONLY)
```
12. Paste this text to config file
```
Host final-project-group3
	HostName <external-ip>
	User <username>
	IdentityFile <your-ssh-private-path>
```
13. Open new terminal to try it out
```
ssh final-project-group3
```
14. Open VSCode and go to the Extension menu
15. Search "Remote - SSH" and install
16. On the left corner of VSCode, click >< icon (Open a remote window)
17. Click Connect to Host SSH - Remote
18. Click your hostname
19. Please wait... And choose Linux for OS
20. Tadaaa... You're successfully connected to VM Instance!