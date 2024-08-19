# Deploy Script
# Description: Deploy and destroy Terraform
# WARNING: This will most likely destroy any current infrastructure if protections
# are not in place. Be careful!

# Exit if any command has a non-zero status
set -e

echo "WARNING: This script will destroy any infrastructure for testing."
echo "It should not be used once a proper deployment has been setup."
echo "Would you like to continue?"

select yn in "Yes" "No"; do
	case $yn in
		Yes ) cd ../terraform/; terraform destroy -auto-approve; terraform apply -auto-approve; terraform destroy -auto-approve; break;;
		No ) exit;;
	esac
done
