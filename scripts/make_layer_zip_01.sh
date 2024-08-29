# Description: Make the zip file for the layer for the extract lambda function

cd "$(dirname "$0")/.."
mkdir -p python_01/lib/python3.11/site-packages
pip3 install --upgrade -r requirements_01.txt -t python_01/lib/python3.11/site-packages
rm layer_01.zip
zip -r layer_01.zip python_01
rm -r python_01/
