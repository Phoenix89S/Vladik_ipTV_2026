name: VK IPTV M3U Collector

on:
  workflow_dispatch:

  schedule:
    - cron: "0 3 * * *"

permissions:
  contents: write

concurrency:
  group: vk-iptv-collector
  cancel-in-progress: false

jobs:
  collect:
    runs-on: ubuntu-latest

    steps:
      # ============================================================
      # 1. CHECKOUT
      # ============================================================
      - name: Checkout repository
        uses: actions/checkout@v7
        with:
          fetch-depth: 0

      # ============================================================
      # 2. PYTHON
      # ============================================================
      - name: Setup Python
        uses: actions/setup-python@v6
        with:
          python-version: "3.12"
          cache: "pip"

      # ============================================================
      # 3. INSTALL DEPENDENCIES
      # ============================================================
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests

      # ============================================================
      # 4. DOWNLOAD EXACT SCRIPT FROM REPOSITORY
      # ============================================================
      - name: Download VK IPTV collector
        run: |
          curl -fL \
            "https://raw.githubusercontent.com/Phoenix89S/Vladik_ipTV_2026/main/VK_IPTV_M3U_COLLECTOR.py" \
            -o VK_IPTV_M3U_COLLECTOR.py

          echo "=== SCRIPT CHECK ==="
          ls -lh VK_IPTV_M3U_COLLECTOR.py
          head -n 10 VK_IPTV_M3U_COLLECTOR.py

      # ============================================================
      # 5. RUN COLLECTOR
      # ============================================================
      - name: Run VK IPTV M3U Collector
        run: |
          python VK_IPTV_M3U_COLLECTOR.py

      # ============================================================
      # 6. SHOW RESULTS
      # ============================================================
      - name: Show collector results
        if: always()
        run: |
          echo "========================================"
          echo "VK IPTV OUTPUT"
          echo "========================================"

          if [ -d "vk_iptv_output" ]; then
            find vk_iptv_output -maxdepth 1 -type f -printf '%f %s bytes\n' | sort
          else
            echo "ERROR: vk_iptv_output directory not found"
            exit 1
          fi

      # ============================================================
      # 7. GIT STATUS
      # ============================================================
      - name: Check Git changes
        run: |
          git status --short

      # ============================================================
      # 8. COMMIT RESULTS
      # ============================================================
      - name: Commit collector results
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

          git add VK_IPTV_M3U_COLLECTOR.py
          git add vk_iptv_output/

          if git diff --cached --quiet; then
            echo "No changes to commit."
            exit 0
          fi

          git commit -m "Update VK IPTV collector results"

      # ============================================================
      # 9. PUSH TO REPOSITORY
      # ============================================================
      - name: Push results
        run: |
          git push