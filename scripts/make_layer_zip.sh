# Description: Make the zip file for the layer for the extract lambda function

cd "$(dirname "$0")/.."

# Layer 01
mkdir -p python/lib/python3.11/site-packages
pip3 install --upgrade -r requirements_lambda_01.txt -t python/lib/python3.11/site-packages
rm layer_01.zip
zip -r layer_01.zip python
rm -r python/

# Layer 02
mkdir -p python/lib/python3.11/site-packages
pip3 install --upgrade -r requirements_lambda_02.txt -t python/lib/python3.11/site-packages
rm layer_02.zip
zip -r layer_02.zip python
rm -r python/
