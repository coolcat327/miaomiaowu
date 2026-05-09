package handler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"miaomiaowu/internal/logger"
	"miaomiaowu/internal/notify"
	"miaomiaowu/internal/storage"
)

func StartNotifyScheduler(ctx context.Context, repo *storage.TrafficRepository) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	var lastDailyRun string

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n := GetNotifier()
			if n == nil {
				continue
			}
			cfg := n.GetConfig()

			if cfg.NotifyDailyTraffic {
				today := now.Format("2006-01-02")
				nowTime := now.Format("15:04")
				targetTime := cfg.DailyTrafficTime
				if targetTime == "" {
					targetTime = "08:00"
				}
				if nowTime == targetTime && lastDailyRun != today {
					lastDailyRun = today
					go sendDailyTrafficNotification(ctx, repo, n)
				}
			}

			if cfg.NotifyExpiry && now.Format("15:04") == "09:00" {
				go sendExpiryNotification(ctx, repo, n)
			}
		}
	}
}

func sendDailyTrafficNotification(ctx context.Context, repo *storage.TrafficRepository, n *notify.Notifier) {
	records, err := repo.ListRecent(ctx, 1)
	if err != nil {
		logger.Warn("[Notify] 获取流量记录失败", "error", err)
		return
	}

	if len(records) == 0 {
		return
	}

	rec := records[0]
	usedGB := float64(rec.TotalUsed) / (1024 * 1024 * 1024)
	limitGB := float64(rec.TotalLimit) / (1024 * 1024 * 1024)
	remainGB := float64(rec.TotalRemaining) / (1024 * 1024 * 1024)

	msg := fmt.Sprintf("已用: %.2f GB / %.2f GB\n剩余: %.2f GB", usedGB, limitGB, remainGB)
	_ = n.Send(ctx, notify.Event{
		Type:    notify.EventDailyTraffic,
		Title:   "每日流量统计",
		Message: msg,
	})
}

func sendExpiryNotification(ctx context.Context, repo *storage.TrafficRepository, n *notify.Notifier) {
	files, err := repo.ListSubscribeFiles(ctx)
	if err != nil {
		logger.Warn("[Notify] 获取订阅文件失败", "error", err)
		return
	}

	now := time.Now()
	threeDaysLater := now.Add(3 * 24 * time.Hour)

	var lines []string
	for _, f := range files {
		if f.ExpireAt == nil {
			continue
		}
		if f.ExpireAt.After(now) && f.ExpireAt.Before(threeDaysLater) {
			days := int(f.ExpireAt.Sub(now).Hours() / 24)
			lines = append(lines, fmt.Sprintf("• %s: %d 天后到期", f.Name, days))
		}
	}

	if len(lines) == 0 {
		return
	}

	msg := strings.Join(lines, "\n")
	_ = n.Send(ctx, notify.Event{
		Type:    notify.EventExpiry,
		Title:   "订阅即将到期",
		Message: msg,
	})
}
