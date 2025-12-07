
df_norm <- NormalizeOutcome(departure_panel_nyt, "total_downloads", "total_downloads_norm")
df_means <- df_norm %>%
  mutate(event_time = time_index - treatment_group) %>%
  group_by(treatment_group, event_time) %>%
  summarize(mean_total_downloads_norm = mean(total_downloads_norm, na.rm = TRUE), .groups = "drop") %>%
  filter(treatment_group >= 8) %>%
  mutate(time_index = event_time + treatment_group,
         treatment_group = as.factor(treatment_group))
ggplot(df_means %>% filter(abs(event_time) <= 7), aes(x = event_time, y = mean_total_downloads_norm, color = treatment_group, group = treatment_group)) +
  geom_line(size = 1) +
  geom_point() +
  labs(title = "Transformed Downloads",
       x = "Event Time",
       y = "Time. Prop Project Normalized Downloads") +
  scale_color_brewer(palette = "Paired") +
  theme_minimal()

df_et_mean <- df_norm %>%
  group_by(time_index) %>%
  summarize(event_time_mean = mean(total_downloads_norm, na.rm = TRUE), .groups = "drop")
df_time_fe <- df_norm %>%
  mutate(event_time = time_index - treatment_group) %>%
  select(event_time, time_index, total_downloads_norm) %>%
  left_join(df_et_mean) %>%
  group_by(event_time) %>%
  summarize(outcome = mean(total_downloads_norm - event_time_mean, na.rm = T))
ggplot(df_time_fe %>% filter(abs(event_time) <= 7), aes(x = event_time, y = outcome)) +
  geom_line(size = 1) +
  geom_point() +
  labs(title = "Demean Transformed Downloads over Event Time",
       x = "Event Time",
       y = "Demeaned Time. Prop Project Normalized Downloads") +
  scale_color_brewer(palette = "Paired") +
  theme_minimal()

df_norm_pct <- df_norm %>%
  group_by(repo_name) %>%
  arrange(time_index) %>% 
  mutate(pct_diff = (total_downloads_norm / lag(total_downloads_norm) - 1) * 100) %>%
  ungroup()
df_et_mean_pct <- df_norm_pct %>%
  group_by(time_index) %>%
  summarize(event_time_mean = mean(pct_diff, na.rm = TRUE), .groups = "drop")
df_time_fe <- df_norm_pct %>%
  mutate(event_time = time_index - treatment_group) %>%
  select(event_time, time_index, pct_diff) %>%
  left_join(df_et_mean_pct, by = "time_index") %>%
  group_by(event_time) %>%
  summarize(outcome = mean(pct_diff - event_time_mean, na.rm = TRUE), .groups = "drop")

ggplot(df_time_fe %>% filter(abs(event_time) <= 7), aes(x = event_time, y = outcome)) +
  geom_line(size = 1) +
  geom_point() +
  labs(title = "Average Demeaned % Difference in Transformed Downloads Over Event Time",
       x = "Event Time",
       y = "Average Demeaned % Difference") +
  theme_minimal()
