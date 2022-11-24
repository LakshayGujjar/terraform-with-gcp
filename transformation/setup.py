import setuptools

setuptools.setup(
           name='Installing Packages',
              version='1.0.0',
                 install_requires=['google-cloud-secret-manager==0.2.0','google-oauth2-tool==0.0.3','google-auth==1.30.0','google-cloud-core==1.4.1','pandas','fsspec','google-cloud-bigquery','gcsfs', 'google.cloud.storage==1.21.0', 'google-auth-oauthlib==0.5.2','apache-beam[gcp]==2.24.0',"pyyaml"],
                    packages=setuptools.find_packages())