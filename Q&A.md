1.从ACV源码基础上修改，具体修改内容可查阅commit

2.Exception: Promptflow may not installed correctly. If you are upgrading from 'promptflow<1.8.0' to 'promptflow>=1.8.0', please run 'pip uninstall -y promptflow promptflow-core promptflow-devkit promptflow-azure', then 'pip install promptflow>=1.8.0'. Reach https://microsoft.github.io/promptflow/how-to-guides/faq.html#promptflow-1-8-0-upgrade-guide for more information.
此问题是否是环境安装问题还待进一步验证

3.httpx.ConnectError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate in certificate chain (_ssl.c:1016)证书问题以及解决思路
~目录下有三个华为证书：
HuaweiWebSecureInternetGatewayCA	通过代理访问外网 HTTPS
HuaweiWebSecureInternetGatewayCAV2	通过代理访问外网 HTTPS
HuaweiSecureInternetPorxyCA	通过代理访问外网 HTTPS

合并 Python 公共 CA （我是把虚拟环境acv_llm的证书合并了，不是虚拟环境base）和三个华为 CA
CERTIFI_CA=$(python -m certifi)
echo "Python certifi CA: $CERTIFI_CA"
然后合并：
cat \
  "$CERTIFI_CA" \
  /home/aarc/zhoushengbo/HuaweiWebSecureInternetGatewayCA.pem \
  /home/aarc/zhoushengbo/HuaweiWebSecureInternetGatewayCAV2.pem \
  /home/aarc/zhoushengbo/HuaweiSecureInternetPorxyCA.pem \
  > /home/aarc/zhoushengbo/huawei-ca-bundle.pem
设置读取权限：
chmod 644 /home/aarc/zhoushengbo/huawei-ca-bundle.pem
设置为 Conda 环境永久生效
创建 Conda 激活脚本：
mkdir -p "$CONDA_PREFIX/etc/conda/activate.d"
写入证书配置：
cat > "$CONDA_PREFIX/etc/conda/activate.d/huawei-ca.sh" <<'EOF'
export SSL_CERT_FILE="/home/aarc/zhoushengbo/huawei-ca-bundle.pem"
export REQUESTS_CA_BUNDLE="/home/aarc/zhoushengbo/huawei-ca-bundle.pem"
export CURL_CA_BUNDLE="/home/aarc/zhoushengbo/huawei-ca-bundle.pem"
EOF