#!/bin/bash
version='v1.0.0'
uri='882334376932.dkr.ecr.ap-southeast-2.amazonaws.com'
function_name="jens-file-moving-app"
REGION="ap-southeast-2"
profile="msa_sandbox"
account="882334376932"

TEMPLATE="template.yaml"

# Build the docker images
docker build --platform linux/amd64 --provenance=false -t $function_name -t $function_name:$version -f dockerfiles/Dockerfile.example .

# Login to ecr and make repo
aws ecr get-login-password --region ap-southeast-2 --profile $profile | docker login --username AWS --password-stdin $uri
aws ecr create-repository --repository-name $function_name --region ap-southeast-2 --profile $profile --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE

# Tag and push the images
docker tag $function_name:latest $uri/$function_name:latest
docker tag $function_name:$version $uri/$function_name:$version

docker push $uri/$function_name:latest
docker push $uri/$function_name:$version

# Update the functions with the new URIs/SHA keys
set -e

# Map: Logical name in SAM template → ECR repository name
functions_list=(
  "ExampleLambdaFunction", "NewLambdaFunction"
)

# Get the latest image digest for the function
digest=$(aws ecr describe-images \
  --repository-name "$function_name" \
  --query 'sort_by(imageDetails,& imagePushedAt)[-1].imageDigest' \
  --output text \
  --region "$REGION" \
  --profile "$profile")


# Check if the digest was retrieved successfully
if [[ "$digest" == "None" || -z "$digest" ]]; then
  echo "No image digest found for $function_name"
  echo "Please check if the image was pushed successfully."
  exit 1
fi

for function_name_updated in "${functions_list[@]}"; do
  # Get the ECR repository name from the logical ID
  echo "Updating function: $function_name_updated"
  # Construct the new ImageUri
  new_uri="${uri}/${function_name}@${digest}"

  # Update the ImageUri in the template file
  sed -i.bak -E "s|(^\s*ImageUri:\s*)${account}\.dkr\.ecr\.${REGION}\.amazonaws\.com/${function_name}[^\"']*|\1$new_uri|" "$TEMPLATE"
done

echo "Deploying the YAML..."
echo "I repeat, deploying the YAML..."
'C:\Program Files\Amazon\AWSSAMCLI\bin\sam.cmd' deploy --resolve-image-repos --profile $profile --config-file samconfig.toml --template template.yaml


