# Description: Make the zip file for the layer for the transform & load
# lambda functions

cd "$(dirname "$0")/.."
mkdir -p python_02/lib/python3.11/site-packages
pip3 install --upgrade -r requirements_lambda_02.txt -t python_02/lib/python3.11/site-packages
rm layer_02.zip
zip -r layer_02.zip python_02
rm -r python_02/
