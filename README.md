# FlipkratReviewScrapper
 
### Project Execution steps
#### Clone repository in local
```bash
git clone <repository_url>
```
#### Execute below commands in git bash. (Note - Must have conda installed and path defined of conda in system environments)
#### Step 1 : Create new conda environment
```bash
conda create --prefix ./env python=3.7
```
#### Step 2 : Activate same environment
```bash
source activate ./env
```
#### Step 3 : Install required packages
```bash
pip install -r requirements.txt
```
#### Step 4 : To save your version of code. Create new git repository & then execute below commands only once to push change first time.
```bash
git add .
git status
git commit -m "commit message"
git remote add origin 'your_repository_url'
git branch -m master main
git push -u origin main
```
#### After first commit for subsequent changes just execute below commands
```bash
git add .
git commit -m "commit message"
git push
```
