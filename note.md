
git status 警告
```
➜  leveldb git:(main) git status

On branch main

Your branch is up to date with 'origin/main'.


Changes not staged for commit:

  (use "git add ..." to update what will be committed)

  (use "git restore ..." to discard changes in working directory)

  (commit or discard the untracked or modified content in submodules)

        modified:   third_party/benchmark (modified content)

        modified:   third_party/googletest (modified content, untracked content)

        modified:   third_party/spdlog (modified content)


no changes added to commit (use "git add" and/or "git commit -a")​
```

解决方案

```shell
git submodule status
cd third_party/googletest
git status  # 查看具体修改内容
git add <修改的文件>
git commit -m "change some code "
cd ../..
cd third_party/benchmark
git status  # 查看具体修改内容
git add <修改的文件>
git commit -m "change some code "
cd ../..
cd third_party/spdlog
git status  # 查看具体修改内容
git add <修改的文件>
git commit -m "change some code "
cd ../..
git add third_party/benchmark third_party/googletest third_party/spdlog
git commit -m "Update third-party dependencies"
git push
```

```shell
git status --ignore-submodules # 忽略子模块修改

```