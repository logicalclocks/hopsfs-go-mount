#!/bin/bash

resolve_test_selection() {
  local repo_root=$1
  local test_file=${2:-}
  local test=${3:-}
  local test_package=${4:-}
  local test_file_path=

  if [ -n "$test_file" ]; then
    if [ -f "$test_file" ]; then
      case "$test_file" in
        /*)
          test_file_path="$test_file"
          ;;
        *)
          test_file_path="$repo_root/$test_file"
          ;;
      esac
    elif [ -f "$repo_root/$test_file" ]; then
      test_file_path="$repo_root/$test_file"
    else
      echo "TEST_FILE '$test_file' does not exist." >&2
      return 1
    fi

    case "$test_file_path" in
      "$repo_root"/*)
        test_package="./$(dirname "${test_file_path#$repo_root/}")"
        ;;
      *)
        echo "TEST_FILE '$test_file_path' must be inside the repository root." >&2
        return 1
        ;;
    esac

    if [ -z "$test" ]; then
      test=$(grep -E '^func Test' "$test_file_path" | sed -E 's/^func (Test[[:alnum:]_]+).*/\1/' | awk 'BEGIN { first = 1 } { if (!first) printf("|"); printf "%s", $0; first = 0 }')
      if [ -z "$test" ]; then
        echo "No Go tests found in '$test_file_path'." >&2
        return 1
      fi
    fi
  fi

  printf '%s\n%s\n%s\n' "$test_file_path" "$test_package" "$test"
}
