import json, boto3, time
import cfnresponse

quicksight = boto3.client('quicksight')

def lambda_handler(event, context):
    resourceName = 'WorkshopSetup'
    awsAccountId = context.invoked_function_arn.split(':')[4]

    def create_workshop_setup():
        nonlocal awsAccountId
        try:
            response = quicksight.create_account_subscription(
                Edition='ENTERPRISE',
                AuthenticationMethod='IAM_AND_QUICKSIGHT',
                AwsAccountId=awsAccountId,
                AccountName='QSWS-'+awsAccountId+'-'+str(int(time.time())),
                NotificationEmail='QSWS-'+awsAccountId+'@workshop.aws'
            )
            print("create account subscription success.")
        except Exception as e:
            return (False, "Error encountered: " + str(e))

        time.sleep(10)
        while True:
            response = quicksight.describe_account_subscription(
                AwsAccountId=awsAccountId
            )
            accountSubscriptionStatus = response['AccountInfo']['AccountSubscriptionStatus']
            if accountSubscriptionStatus == 'ACCOUNT_CREATED':
                break
            time.sleep(5)
        # create namespace and register user
        try:
            # regester user to a AUTHOR_PRO
            response = quicksight.register_user(
                IdentityType='IAM',
                Email='demouser@quicksightadminworkshop.com',
                UserRole='AUTHOR_PRO',
                # IamArn='arn:aws:iam::'+awsAccountId+':role/TrialUseOnly-ContentGeneratedByGenAIDoesNotRepresentViewsOfAWS',
                IamArn='arn:aws:iam::'+awsAccountId+':user/quicksight-admin',
                AwsAccountId=awsAccountId,
                Namespace='default'
            )
            return(True, "create namespace and register user success.")
        except Exception as e:
            return (False, "Register User Error encountered: " + str(e))
        return(True, "Workshop setup initiated.")

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
        print('register user')
    elif event['RequestType'] == 'Update':
        result, reason = (True, "Update method not implemented")
    elif event['RequestType'] == 'Delete':
        result, reason = delete_workshop_setup()
    else:
        result = False
        reason = "Unknown operation: " + event['RequestType']

    print(result)
    print(reason)

    responseData = {}
    responseData['Reason'] = reason
    if result:
        cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData, resourceName)
    else:
        cfnresponse.send(event, context, cfnresponse.FAILED, responseData, resourceName)