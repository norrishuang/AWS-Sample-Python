import json, boto3, time
import cfnresponse

quicksight = boto3.client('quicksight')

def lambda_handler(event, context):
    resourceName = 'WorkshopSetup'
    awsAccountId = context.invoked_function_arn.split(':')[4]

    def create_workshop_setup():
        nonlocal awsAccountId
        # exists account subscription
        response = quicksight.describe_account_subscription(
            AwsAccountId=awsAccountId
        )
        accountSubscriptionStatus = response['AccountInfo']['AccountSubscriptionStatus']
        quicksightAccountName = response['AccountInfo']['AccountName']

        # print(accountSubscriptionStatus)
        # return(True, "Get Status SUCCESS.")


        if accountSubscriptionStatus != 'ACCOUNT_CREATED':
            # if not craeted, to craete
            try:
                quicksight.create_account_subscription(
                    Edition='ENTERPRISE',
                    AuthenticationMethod='IAM_AND_QUICKSIGHT',
                    AwsAccountId=awsAccountId,
                    AccountName='QSWS-'+awsAccountId+'-'+str(int(time.time())),
                    NotificationEmail='QSWS-'+awsAccountId+'@workshop.aws'
                )
                return(True, "Workshop setup initiated.")
            except Exception as e:
                return (False, "Error encountered: " + str(e))

        # create namespace and register user
        try:
            response = quicksight.create_namespace(
                AwsAccountId=awsAccountId,
                Namespace=quicksightAccountName,
                IdentityStore='QUICKSIGHT'
            )
            iam = boto3.client('iam')
            userName = iam.get_user()['User']['UserName']

            # regester user to a AUTHOR_PRO
            response = quicksight.register_user(
                IdentityType='IAM',
                Email=userName+'@quicksightadminworkshop.com',
                UserRole='AUTHOR_PRO',
                IamArn='arn:aws:iam::'+awsAccountId+':user/'+userName,
                AwsAccountId=awsAccountId,
                Namespace=awsAccountId,
                UserName=userName
            )
            return(True, "create namespace and register user success.")
        except Exception as e:
            return (False, "Error encountered: " + str(e))


    def delete_workshop_setup():
        nonlocal awsAccountId
        try:
            iam = boto3.client('iam')
            userName = iam.get_user()['User']['UserName']
            response = quicksight.delete_namespace(
                AwsAccountId=awsAccountId,
                Namespace=userName
            )


            quicksight.update_account_settings(
                AwsAccountId=awsAccountId,
                DefaultNamespace='default',
                TerminationProtectionEnabled=False
            )
            quicksight.delete_account_subscription(
                AwsAccountId=awsAccountId
            )
            return (True, "Workshop setup delete initiated.")
        except Exception as e:
            return (False, "Error encountered: " + str(e))


    if event['RequestType'] == 'Create':
        result, reason = create_workshop_setup()
    elif event['RequestType'] == 'Update':
        result, reason = (True, "Update method not implemented")
    elif event['RequestType'] == 'Delete':
        result, reason = delete_workshop_setup()
    else:
        result = False
        reason = "Unknown operation: " + event['RequestType']

    responseData = {}
    responseData['Reason'] = reason
    if result:
        cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData, resourceName)
    else:
        cfnresponse.send(event, context, cfnresponse.FAILED, responseData, resourceName)