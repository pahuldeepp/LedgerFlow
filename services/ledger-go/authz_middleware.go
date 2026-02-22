package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
)



const (
	hUserID     = "X-User-ID"
	hUserGroups = "X-User-Groups"
)

func RequireAuth() gin.HandlerFunc{
	return func(c *gin.Context){
		user := strings.TrimSpace(c.GetHeader(hUserID))
		if user == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			c.Abort()
			return
		}
		
		c.Next()
	}
}
func RequireAnyGroup(allowed ...string) gin.HandlerFunc {
	allowedSet := map[string]struct{}{}
	for _, a := range allowed {
		allowedSet[strings.ToLower(strings.TrimSpace(a))] = struct{}{}
	}
	return func(c *gin.Context) {
		user := strings.TrimSpace(c.GetHeader(hUserID))
		if user == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			c.Abort()
			return
		}
		groups := parseGroups(c.GetHeader(hUserGroups))
		for _, g := range groups {
			if _, ok := allowedSet[g]; ok {
				c.Next()
				return
			}
		}

		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		c.Abort()
	}
}

func parseGroups(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	// oauth2-proxy groups can be "a,b,c" or "a b c"
	raw = strings.ReplaceAll(raw, ",", " ")
	fields := strings.Fields(raw)

	out := make([]string, 0, len(fields))
	seen := map[string]struct{}{}
	for _, f := range fields {
		f = strings.ToLower(strings.TrimSpace(f))
		if f == "" {
			continue
		}
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		out = append(out, f)
	}
	return out
}