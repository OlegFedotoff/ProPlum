import subprocess
import os

def get_current_git_branch():
    """
    Получает название текущей git-ветки.
    Возвращает (branch_name, error_message).
    Если ошибка, то branch_name = None.
    """
    try:
        # Проверяем, что мы находимся в git-репозитории
        result = subprocess.run(['git', 'rev-parse', '--is-inside-work-tree'], 
                              capture_output=True, text=True, cwd=os.getcwd())
        if result.returncode != 0:
            return None, "Current directory is not a git repository"
        
        # Получаем название текущей ветки
        result = subprocess.run(['git', 'branch', '--show-current'], 
                              capture_output=True, text=True, cwd=os.getcwd())
        if result.returncode != 0:
            return None, f"Failed to get current git branch: {result.stderr.strip()}"
        
        branch_name = result.stdout.strip()
        if not branch_name:
            return None, "Could not determine current git branch (detached HEAD?)"
        
        return branch_name, None
    
    except FileNotFoundError:
        return None, "Git is not installed or not available in PATH"
    except Exception as e:
        return None, f"Unexpected error while getting git branch: {str(e)}"

def validate_set_with_git_branch(set_name):
    """
    Проверяет, что указанный set_name соответствует текущей git-ветке.
    Возвращает (is_valid, error_message).
    """
    current_branch, error = get_current_git_branch()
    if error:
        return False, error
    
    if set_name != current_branch:
        return False, f"Set name '{set_name}' does not match current git branch '{current_branch}'"
    
    return True, None 