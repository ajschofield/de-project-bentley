# Description: Make the zip file for the layer

cd "$(dirname "$0")/.."
mkdir -p python/lib/python3.11/site-packages
pip3 install --upgrade -r requirements.txt -t python/lib/python3.11/site-packages
rm layer.zip
zip -r layer.zip python
rm -r python/
