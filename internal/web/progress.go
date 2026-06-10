package web

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

type ProgressHandler struct {
	store domain.StateStore
}

func NewProgressHandler(store domain.StateStore) *ProgressHandler {
	return &ProgressHandler{store: store}
}

func (h *ProgressHandler) RegisterRoutes(r *gin.Engine) {
	r.GET("/users/:userId/rules/:ruleId/progress", h.GetProgress)
}

func (h *ProgressHandler) GetProgress(c *gin.Context) {
	userID, err := strconv.ParseInt(c.Param("userId"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid userId"})
		return
	}
	ruleID, err := strconv.ParseInt(c.Param("ruleId"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ruleId"})
		return
	}
	biz := c.Query("biz")
	if biz == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "biz is required"})
		return
	}

	progress, err := h.store.GetProgress(c.Request.Context(), biz, userID, ruleID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, progress)
}
