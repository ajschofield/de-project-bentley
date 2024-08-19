# Deploy Script
# Description: Deploy and destroy Terraform
# WARNING: This will most likely destroy any current infrastructure if protections
# are not in place. Be careful!

# Exit if any command has a non-zero status
set -e

# Change current directory to terraform folder at the start
cd ../terraform/

echo "WARNING: This script will destroy any infrastructure for testing."
echo "It should not be used once a proper deployment has been setup."
echo "Would you like to continue?"

select yn in "Yes" "No"; do
    case $yn in
    Yes)
        echo "Would you like to destroy the current infrastructure?"
        select destroy_1 in "Yes" "No"; do
            case $destroy_1 in
            Yes)
                terraform destroy
                break
                ;;
            No)
                echo "Skipping initial destroy..."
                break
                ;;
            esac
        done

        terraform apply

        echo "Would you like to destroy the newly-created infrastructure?"
        select destroy_2 in "Yes" "No"; do
            case $destroy_2 in
            Yes)
                terraform destroy
                break
                ;;
            No)
                echo "Skipping final destroy... Infrastructure will remain."
                break
                ;;
            esac
        done

        break
        ;;
    No)
        echo "Operation cancelled..."
        exit
        ;;
    esac
done
