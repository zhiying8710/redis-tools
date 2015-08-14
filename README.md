# redis-tools
打包jar-with-dependencies: mvn assembly:assembly, <br/>
打开.m2/\org\apache\maven\plugins\maven-assembly-plugin\{version}\maven-assembly-plugin-{version}.jar,<br/>
找到assemblies\jar-with-dependencies.xml, 把里面的UNPACK改成FALSE
